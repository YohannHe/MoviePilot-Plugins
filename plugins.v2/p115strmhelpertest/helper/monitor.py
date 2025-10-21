import threading
import traceback
import shutil
import re
import time
from pathlib import Path
from collections import defaultdict
from typing import Optional, Any

from watchdog.events import FileSystemEventHandler

from app.chain.storage import StorageChain
from app.log import logger
from app.utils.system import SystemUtils
from app.schemas import FileItem

from ..core.config import configer
from ..db_manager.models.upload_queue import UploadQueue
from ..db_manager.models.upload_history import UploadHistory


directory_upload_dict = defaultdict(threading.Lock)


class FileMonitorHandler(FileSystemEventHandler):
    """
    目录监控响应类
    """

    def __init__(self, monpath: str, sync: Any, **kwargs):
        super(FileMonitorHandler, self).__init__(**kwargs)
        self._watch_path = monpath
        self.sync = sync

    def on_created(self, event):
        """
        创建事件（PollingObserver 检测到新文件）
        """
        if not event.is_directory:
            logger.info(
                f"【监控事件】✓ PollingObserver 检测到文件创建 | "
                f"路径: {event.src_path} | "
                f"监控目录: {self._watch_path}"
            )
        else:
            logger.debug(
                f"【监控事件】检测到目录创建（忽略）| 路径: {event.src_path}"
            )
        
        self.sync.event_handler(
            event=event,
            text="创建",
            mon_path=self._watch_path,
            event_path=event.src_path,
        )

    def on_moved(self, event):
        """
        移动/重命名事件（PollingObserver 检测到文件改名）
        """
        if not event.is_directory:
            logger.info(
                f"【监控事件】✓ PollingObserver 检测到文件移动/改名 | "
                f"从: {event.src_path} | "
                f"到: {event.dest_path} | "
                f"监控目录: {self._watch_path}"
            )
        else:
            logger.debug(
                f"【监控事件】检测到目录移动（忽略）| 从: {event.src_path} 到: {event.dest_path}"
            )
        
        self.sync.event_handler(
            event=event,
            text="移动",
            mon_path=self._watch_path,
            event_path=event.dest_path,
        )


def is_file_stable(file_path: Path, wait_seconds: int = 5) -> bool:
    """
    检查文件是否稳定（大小不再变化）
    
    适用于 rclone 挂载目录，等待下载完成
    
    :param file_path: 文件路径
    :param wait_seconds: 等待秒数
    :return: True 如果文件稳定，否则 False
    """
    try:
        if not file_path.exists():
            return False
            
        size1 = file_path.stat().st_size
        time.sleep(wait_seconds)
        
        if not file_path.exists():
            return False
            
        size2 = file_path.stat().st_size
        stable = (size1 == size2 and size1 > 0)
        
        if stable:
            logger.debug(
                f"【文件稳定性】文件稳定: {file_path.name} | 大小: {size1 / 1024 / 1024:.2f}MB"
            )
        else:
            logger.info(
                f"【文件稳定性】文件未稳定: {file_path.name} | 大小变化: {size1} -> {size2}"
            )
            
        return stable
        
    except Exception as e:
        logger.error(f"【文件稳定性】检查失败: {file_path.name} | 错误: {e}")
        return False


def enqueue_file(event_path: str, mon_path: str):
    """
    文件入队处理函数
    
    核心改变：不再直接执行上传，而是加入数据库队列
    
    流程：
    1. 文件有效性检查（回收站、蓝光、稳定性）
    2. 生成文件指纹（大小+修改时间）
    3. 去重检查（历史记录+队列）
    4. 匹配监控配置（获取目标路径）
    5. 判断文件类型（上传/复制）
    6. 插入队列（状态=pending）
    
    :param event_path: 事件文件路径
    :param mon_path: 监控目录
    """
    file_path = Path(event_path)
    
    logger.info(
        f"【目录上传】>>> 开始处理文件 | "
        f"文件名: {file_path.name} | "
        f"完整路径: {event_path}"
    )
    
    try:
        # === 1. 基础检查 ===
        
        if not file_path.exists():
            logger.debug(f"【目录上传】文件不存在，忽略: {event_path}")
            return
            
        # 全程加锁（保持原有逻辑）
        with directory_upload_dict[str(file_path.absolute())]:
            
            # 回收站/隐藏文件不处理
            if (
                "/@Recycle/" in event_path
                or "/#recycle/" in event_path
                or "/." in event_path
                or "/@eaDir" in event_path
            ):
                logger.debug(f"【目录上传】回收站或隐藏文件，忽略: {event_path}")
                return

            # 蓝光目录不处理
            if re.search(r"BDMV[/\\]STREAM", event_path, re.IGNORECASE):
                logger.debug(f"【目录上传】蓝光目录文件，忽略: {event_path}")
                return

            # 文件稳定性检查（新增，针对 rclone 挂载）
            logger.info(f"【目录上传】等待文件稳定: {file_path.name}")
            if not is_file_stable(file_path, wait_seconds=5):
                logger.info(f"【目录上传】文件未稳定（可能在下载中），跳过: {file_path.name}")
                return

            # === 2. 生成文件指纹 ===
            
            stat = file_path.stat()
            file_size = stat.st_size
            file_mtime = stat.st_mtime
            fingerprint = f"{file_size}_{int(file_mtime)}"
            
            logger.info(
                f"【目录上传】文件信息 | "
                f"大小: {file_size / 1024 / 1024:.2f}MB | "
                f"指纹: {fingerprint}"
            )

            # === 3. 去重检查 ===
            
            # 3.1 检查历史记录
            if UploadHistory.is_uploaded(fingerprint, file_size):
                logger.info(
                    f"【目录上传】文件已上传过（历史去重），跳过: {file_path.name}"
                )
                return

            # 3.2 检查队列
            if UploadQueue.exists_in_queue(str(file_path)):
                logger.info(
                    f"【目录上传】文件已在队列中，跳过: {file_path.name}"
                )
                return

            # === 4. 匹配监控配置 ===
            
            logger.debug("【目录上传】匹配监控路径配置...")
            
            dest_remote = ""
            dest_local = ""
            delete_source = False
            config_matched = False
            
            for item in configer.get_config("directory_upload_path"):
                if not item:
                    continue
                if mon_path == item.get("src", ""):
                    dest_remote = item.get("dest_remote", "")
                    dest_local = item.get("dest_local", "")
                    delete_source = item.get("delete", False)
                    config_matched = True
                    
                    logger.info(
                        f"【目录上传】✓ 匹配到监控配置 | "
                        f"网盘目标: {dest_remote or '未配置'} | "
                        f"本地目标: {dest_local or '未配置'} | "
                        f"删除源文件: {delete_source}"
                    )
                    break
            
            if not config_matched:
                logger.warning(f"【目录上传】未找到匹配的监控配置: {mon_path}")
                return

            # === 5. 判断文件类型 ===
            
            upload_exts = [
                f".{ext.strip()}"
                for ext in configer.get_config("directory_upload_uploadext")
                .replace("，", ",")
                .split(",")
            ]
            copy_exts = [
                f".{ext.strip()}"
                for ext in configer.get_config("directory_upload_copyext")
                .replace("，", ",")
                .split(",")
            ]
            
            file_suffix = file_path.suffix.lower()
            
            logger.debug(
                f"【目录上传】文件后缀: {file_suffix} | "
                f"上传匹配: {upload_exts} | "
                f"复制匹配: {copy_exts}"
            )

            operation_type = None
            target_dest = None
            
            if file_suffix in upload_exts:
                # 需要上传到网盘
                if not dest_remote:
                    logger.warning(
                        f"【目录上传】未配置网盘目标目录，跳过: {file_path.name}"
                    )
                    return
                    
                operation_type = "upload"
                target_dest = dest_remote
                logger.info(f"【目录上传】文件类型: 上传到网盘 -> {dest_remote}")
                
            elif file_suffix in copy_exts:
                # 需要复制到本地
                if not dest_local:
                    logger.debug(
                        f"【目录上传】未配置本地目标目录，跳过: {file_path.name}"
                    )
                    return
                    
                operation_type = "copy"
                target_dest = dest_local
                logger.info(f"【目录上传】文件类型: 复制到本地 -> {dest_local}")
                
            else:
                # 未匹配后缀
                logger.debug(
                    f"【目录上传】文件后缀不匹配，跳过: {file_suffix}"
                )
                return

            # === 6. 插入队列 ===
            
            logger.info(f"【目录上传】准备加入队列: {file_path.name}")
            
            UploadQueue.create_task(
                file_path=str(file_path),
                file_name=file_path.name,
                file_size=file_size,
                file_mtime=file_mtime,
                monitor_path=mon_path,
                operation_type=operation_type,
                dest_remote=dest_remote if operation_type == "upload" else None,
                dest_local=dest_local if operation_type == "copy" else None,
                delete_source=delete_source,
            )
            
            logger.info(
                f"【目录上传】✓ 文件已加入队列: {file_path.name} | "
                f"类型: {operation_type} | "
                f"目标: {target_dest}"
            )

    except Exception as e:
        logger.error(
            f"【目录上传】入队异常: {file_path.name} | {str(e)}\n{traceback.format_exc()}"
        )
