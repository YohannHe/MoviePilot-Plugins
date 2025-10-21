import shutil
import traceback
from pathlib import Path
from typing import Optional, Tuple

from app.chain.storage import StorageChain
from app.log import logger
from app.utils.system import SystemUtils
from app.schemas import FileItem

from ..db_manager.models.upload_queue import UploadQueue
from ..db_manager.models.upload_history import UploadHistory


class UploadQueueProcessor:
    """
    上传任务队列处理器
    
    负责从数据库队列中取出任务并执行上传/复制操作。
    独立运行，不依赖文件监控模块，通过定时任务触发。
    
    核心特性：
    - 串行处理：一次只处理一个任务，避免并发冲突
    - 失败重试：最多重试3次
    - 持久化：任务状态保存在数据库中
    - 去重检查：处理前二次确认文件未上传过
    """

    def __init__(self):
        """初始化处理器"""
        self.storagechain = StorageChain()
        self.is_processing = False  # 处理标志，防止并发
        logger.info("【队列处理器】初始化完成")

    def reset_stale_tasks(self):
        """
        重置僵尸任务（启动时调用）
        
        将所有 processing 状态的任务改回 pending
        这些任务可能是上次程序异常退出时遗留的
        """
        logger.info("【队列处理器】检查僵尸任务...")
        count = UploadQueue.reset_processing_tasks()
        
        if count > 0:
            logger.info(f"【队列处理器】重置了 {count} 个僵尸任务为待处理状态")
        else:
            logger.debug("【队列处理器】无僵尸任务")

    def process_one_task(self) -> bool:
        """
        处理一个待处理任务（主处理流程）
        
        返回: True=处理了任务, False=队列为空
        """
        # 防止并发处理
        if self.is_processing:
            logger.debug("【队列处理器】正在处理中，跳过本次调度")
            return False

        self.is_processing = True
        
        try:
            # 1. 获取任务
            task = UploadQueue.get_next_pending_task()
            if not task:
                # 队列为空，不输出日志（避免刷屏）
                return False

            logger.info(
                f"【队列处理】开始处理任务 #{task.id} | "
                f"文件: {task.file_name} | "
                f"类型: {task.operation_type} | "
                f"大小: {task.file_size / 1024 / 1024:.2f}MB"
            )

            # 2. 标记为处理中
            UploadQueue.mark_as_processing(task.id)

            # 3. 检查文件是否存在
            file_path = Path(task.file_path)
            if not file_path.exists():
                logger.warning(f"【队列处理】文件不存在，标记失败: {task.file_path}")
                UploadQueue.mark_as_failed(task.id, "文件不存在", task.retry_count)
                return True

            # 4. 二次去重检查（防止并发情况）
            fingerprint = f"{task.file_size}_{task.file_mtime}"
            if UploadHistory.is_uploaded(fingerprint, task.file_size):
                logger.info(
                    f"【队列处理】文件已上传过（历史去重），跳过: {task.file_name}"
                )
                UploadQueue.mark_as_completed(task.id)
                return True

            # 5. 执行上传/复制
            success, error_msg = self._execute_task(task, file_path)

            # 6. 更新状态
            if success:
                logger.info(f"【队列处理】✓ 任务执行成功: {task.file_name}")
                UploadQueue.mark_as_completed(task.id)
                
                # 记录到历史表
                UploadHistory.record_upload(
                    fingerprint=fingerprint,
                    file_size=task.file_size,
                    file_path=task.file_path,
                    file_name=task.file_name,
                    operation_type=task.operation_type,
                    dest_remote=task.dest_remote,
                    dest_local=task.dest_local,
                )

                # 删除源文件（如果配置）
                if task.delete_source:
                    self._delete_source_file(file_path, task.monitor_path)
                    
            else:
                # 失败处理
                retry_count = task.retry_count + 1
                
                if retry_count < 3:
                    logger.warning(
                        f"【队列处理】✗ 任务失败，将重试 ({retry_count}/3): {error_msg}"
                    )
                    UploadQueue.mark_as_pending_retry(task.id, retry_count)
                else:
                    logger.error(
                        f"【队列处理】✗ 任务失败，已达最大重试次数: {error_msg}"
                    )
                    UploadQueue.mark_as_failed(task.id, error_msg, retry_count)

            return True

        except Exception as e:
            error_msg = f"处理器异常: {type(e).__name__}: {str(e)}"
            logger.error(f"【队列处理】{error_msg}\n{traceback.format_exc()}")
            return False
            
        finally:
            self.is_processing = False

    def _execute_task(self, task: UploadQueue, file_path: Path) -> Tuple[bool, str]:
        """
        执行具体的上传/复制操作
        
        复用 monitor.py 中的上传逻辑，保持一致性
        
        :param task: 任务对象
        :param file_path: 文件路径对象
        :return: (成功与否, 错误信息)
        """
        try:
            if task.operation_type == "upload":
                logger.info(
                    f"【队列处理】执行上传: {task.file_name} -> {task.dest_remote}"
                )
                return self._do_upload(task, file_path)
                
            elif task.operation_type == "copy":
                logger.info(
                    f"【队列处理】执行复制: {task.file_name} -> {task.dest_local}"
                )
                return self._do_copy(task, file_path)
                
            else:
                return False, f"未知的操作类型: {task.operation_type}"
                
        except Exception as e:
            error_msg = f"{type(e).__name__}: {str(e)}"
            logger.error(f"【队列处理】执行异常: {error_msg}")
            return False, error_msg

    def _do_upload(self, task: UploadQueue, file_path: Path) -> Tuple[bool, str]:
        """
        执行网盘上传
        
        复用 monitor.py 的上传逻辑
        
        :param task: 任务对象
        :param file_path: 文件路径对象
        :return: (成功与否, 错误信息)
        """
        try:
            # 计算目标路径（保持相对路径结构）
            target_file_path = Path(task.dest_remote) / file_path.relative_to(
                task.monitor_path
            )
            
            logger.info(
                f"【队列处理】上传目标路径: {target_file_path}"
            )

            # 网盘目录创建流程
            def __find_dir(_fileitem: FileItem, _name: str) -> Optional[FileItem]:
                """查找下级目录中匹配名称的目录"""
                for sub_folder in self.storagechain.list_files(_fileitem):
                    if sub_folder.type != "dir":
                        continue
                    if sub_folder.name == _name:
                        return sub_folder
                return None

            # 获取或创建目标目录
            target_fileitem = self.storagechain.get_file_item(
                storage="u115", path=target_file_path.parent
            )
            
            if not target_fileitem:
                # 逐级查找和创建目录
                logger.info("【队列处理】目标目录不存在，开始创建...")
                target_fileitem = FileItem(storage="u115", path="/")
                
                for part in target_file_path.parent.parts[1:]:
                    dir_file = __find_dir(target_fileitem, part)
                    if dir_file:
                        logger.debug(
                            f"【队列处理】发现已存在目录: {target_fileitem.path}{part}"
                        )
                        target_fileitem = dir_file
                    else:
                        logger.info(
                            f"【队列处理】创建目录: {target_fileitem.path}{part}"
                        )
                        dir_file = self.storagechain.create_folder(target_fileitem, part)
                        if not dir_file:
                            error_msg = f"创建目录失败: {target_fileitem.path}{part}"
                            logger.error(f"【队列处理】{error_msg}")
                            return False, error_msg
                        target_fileitem = dir_file

            # 执行上传
            logger.info(
                f"【队列处理】开始上传文件到网盘: {target_fileitem.path}"
            )
            
            if self.storagechain.upload_file(target_fileitem, file_path, file_path.name):
                logger.info(
                    f"【队列处理】✓ 文件上传成功: {file_path.name} -> {target_file_path}"
                )
                return True, ""
            else:
                error_msg = "上传失败（storagechain.upload_file 返回 False）"
                logger.error(f"【队列处理】{error_msg}")
                return False, error_msg

        except Exception as e:
            error_msg = f"上传异常: {type(e).__name__}: {str(e)}"
            logger.error(f"【队列处理】{error_msg}\n{traceback.format_exc()}")
            return False, error_msg

    def _do_copy(self, task: UploadQueue, file_path: Path) -> Tuple[bool, str]:
        """
        执行本地复制
        
        复用 monitor.py 的复制逻辑
        
        :param task: 任务对象
        :param file_path: 文件路径对象
        :return: (成功与否, 错误信息)
        """
        try:
            if not task.dest_local:
                return False, "未配置本地目标路径"

            # 计算目标路径（保持相对路径结构）
            target_file_path = Path(task.dest_local) / file_path.relative_to(
                task.monitor_path
            )
            
            logger.info(
                f"【队列处理】复制目标路径: {target_file_path}"
            )

            # 创建本地目录
            target_file_path.parent.mkdir(parents=True, exist_ok=True)

            # 执行复制
            logger.info("【队列处理】开始复制文件...")
            status, msg = SystemUtils.copy(file_path, target_file_path)
            
            if status == 0:
                logger.info(
                    f"【队列处理】✓ 文件复制成功: {file_path.name} -> {target_file_path}"
                )
                return True, ""
            else:
                error_msg = f"复制失败: {msg}"
                logger.error(f"【队列处理】{error_msg}")
                return False, error_msg

        except Exception as e:
            error_msg = f"复制异常: {type(e).__name__}: {str(e)}"
            logger.error(f"【队列处理】{error_msg}\n{traceback.format_exc()}")
            return False, error_msg

    def _delete_source_file(self, file_path: Path, monitor_path: str):
        """
        删除源文件及空目录
        
        复用 monitor.py 的删除逻辑
        
        :param file_path: 文件路径对象
        :param monitor_path: 监控根目录
        """
        try:
            logger.info(f"【队列处理】删除源文件: {file_path}")
            file_path.unlink(missing_ok=True)

            # 递归删除空父目录
            for file_dir in file_path.parents:
                # 不删除监控根目录及以上
                if len(str(file_dir)) <= len(str(Path(monitor_path))):
                    break
                    
                files = SystemUtils.list_files(file_dir)
                if not files:
                    logger.info(f"【队列处理】删除空目录: {file_dir}")
                    shutil.rmtree(file_dir, ignore_errors=True)
                else:
                    # 目录非空，停止删除
                    break

        except Exception as e:
            logger.error(
                f"【队列处理】删除源文件失败: {str(e)}\n{traceback.format_exc()}"
            )

    def get_queue_stats(self):
        """
        获取队列统计信息
        
        用于监控面板和日志输出
        
        :return: 状态统计字典
        """
        stats = UploadQueue.count_by_status()
        
        logger.info(
            f"【队列统计】待处理: {stats.get('pending', 0)} | "
            f"处理中: {stats.get('processing', 0)} | "
            f"已完成: {stats.get('completed', 0)} | "
            f"失败: {stats.get('failed', 0)}"
        )
        
        return stats

