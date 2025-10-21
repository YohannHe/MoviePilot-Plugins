from typing import Optional, Dict, List
from datetime import datetime

from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    BigInteger,
    Boolean,
    select,
    update,
    func,
)
from sqlalchemy.orm import Session

from app.log import logger
from ...db_manager import db_update, db_query, P115StrmHelperBase


class UploadQueue(P115StrmHelperBase):
    """
    文件上传任务队列表
    
    用于持久化待处理的上传任务，支持重启恢复和串行处理。
    每个文件在监控到后会先加入队列，再由队列处理器异步处理。
    
    状态流转：
    pending -> processing -> completed/failed
    failed -> pending (重试)
    """

    __tablename__ = "upload_queue"

    # 主键
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # 文件信息
    file_path = Column(Text, nullable=False, unique=True)  # 源文件绝对路径（唯一）
    file_name = Column(String(255), nullable=False)  # 文件名
    file_size = Column(BigInteger, nullable=False)  # 文件大小（字节）
    file_mtime = Column(BigInteger, nullable=False)  # 文件修改时间戳（用于生成指纹）
    
    # 监控配置
    monitor_path = Column(Text, nullable=False)  # 触发的监控目录
    dest_remote = Column(Text)  # 网盘上传目标路径（可选）
    dest_local = Column(Text)  # 本地复制目标路径（可选）
    operation_type = Column(String(20), nullable=False)  # "upload" 或 "copy"
    delete_source = Column(Boolean, default=False)  # 是否删除源文件
    
    # 任务状态
    status = Column(String(20), nullable=False, default="pending", index=True)  # 任务状态（索引）
    retry_count = Column(Integer, default=0)  # 重试次数
    error_message = Column(Text)  # 错误信息
    
    # 时间戳
    created_at = Column(BigInteger, nullable=False)  # 创建时间戳
    updated_at = Column(BigInteger, nullable=False)  # 更新时间戳

    @staticmethod
    @db_query
    def get_next_pending_task(db: Session) -> Optional["UploadQueue"]:
        """
        获取下一个待处理任务
        
        按创建时间排序，先入先出（FIFO）
        只返回状态为 pending 的任务
        
        :param db: 数据库会话
        :return: 待处理任务对象，如果队列为空则返回 None
        """
        logger.debug("【队列处理】查询待处理任务...")
        
        task = db.execute(
            select(UploadQueue)
            .where(UploadQueue.status == "pending")
            .order_by(UploadQueue.created_at.asc())
            .limit(1)
        ).scalar_one_or_none()
        
        if task:
            logger.debug(f"【队列处理】找到待处理任务: #{task.id} | {task.file_name}")
        
        return task

    @staticmethod
    @db_update
    def mark_as_processing(db: Session, task_id: int):
        """
        标记任务为处理中
        
        :param db: 数据库会话
        :param task_id: 任务ID
        """
        timestamp = int(datetime.now().timestamp())
        db.execute(
            update(UploadQueue)
            .where(UploadQueue.id == task_id)
            .values(status="processing", updated_at=timestamp)
        )
        logger.info(f"【队列处理】任务 #{task_id} 标记为处理中")

    @staticmethod
    @db_update
    def mark_as_completed(db: Session, task_id: int):
        """
        标记任务为已完成
        
        :param db: 数据库会话
        :param task_id: 任务ID
        """
        timestamp = int(datetime.now().timestamp())
        db.execute(
            update(UploadQueue)
            .where(UploadQueue.id == task_id)
            .values(status="completed", updated_at=timestamp)
        )
        logger.info(f"【队列处理】任务 #{task_id} 已完成")

    @staticmethod
    @db_update
    def mark_as_failed(db: Session, task_id: int, error_msg: str, retry_count: int = 0):
        """
        标记任务为失败
        
        :param db: 数据库会话
        :param task_id: 任务ID
        :param error_msg: 错误信息
        :param retry_count: 当前重试次数
        """
        timestamp = int(datetime.now().timestamp())
        db.execute(
            update(UploadQueue)
            .where(UploadQueue.id == task_id)
            .values(
                status="failed",
                error_message=error_msg,
                retry_count=retry_count,
                updated_at=timestamp
            )
        )
        logger.info(f"【队列处理】任务 #{task_id} 失败: {error_msg}")

    @staticmethod
    @db_update
    def mark_as_pending_retry(db: Session, task_id: int, retry_count: int):
        """
        标记任务为待重试（重置为 pending 状态）
        
        :param db: 数据库会话
        :param task_id: 任务ID
        :param retry_count: 重试次数
        """
        timestamp = int(datetime.now().timestamp())
        db.execute(
            update(UploadQueue)
            .where(UploadQueue.id == task_id)
            .values(status="pending", retry_count=retry_count, updated_at=timestamp)
        )
        logger.info(f"【队列处理】任务 #{task_id} 标记为待重试 ({retry_count}/3)")

    @staticmethod
    @db_query
    def exists_in_queue(db: Session, file_path: str) -> bool:
        """
        检查文件是否已在队列中
        
        只要不是 failed 或 completed 状态，就认为在队列中
        （包括 pending 和 processing）
        
        :param db: 数据库会话
        :param file_path: 文件路径
        :return: True 如果在队列中，否则 False
        """
        count = db.execute(
            select(func.count())
            .select_from(UploadQueue)
            .where(
                UploadQueue.file_path == file_path,
                UploadQueue.status.in_(["pending", "processing"])
            )
        ).scalar()
        
        exists = count > 0
        if exists:
            logger.debug(f"【队列检查】文件已在队列中: {file_path}")
        
        return exists

    @staticmethod
    @db_update
    def create_task(
        db: Session,
        file_path: str,
        file_name: str,
        file_size: int,
        file_mtime: float,
        monitor_path: str,
        operation_type: str,
        dest_remote: Optional[str] = None,
        dest_local: Optional[str] = None,
        delete_source: bool = False,
    ) -> "UploadQueue":
        """
        创建新的上传任务
        
        :param db: 数据库会话
        :param file_path: 文件路径
        :param file_name: 文件名
        :param file_size: 文件大小
        :param file_mtime: 文件修改时间
        :param monitor_path: 监控目录
        :param operation_type: 操作类型（upload/copy）
        :param dest_remote: 网盘目标路径
        :param dest_local: 本地目标路径
        :param delete_source: 是否删除源文件
        :return: 创建的任务对象
        """
        timestamp = int(datetime.now().timestamp())
        
        task = UploadQueue(
            file_path=file_path,
            file_name=file_name,
            file_size=file_size,
            file_mtime=int(file_mtime),
            monitor_path=monitor_path,
            dest_remote=dest_remote,
            dest_local=dest_local,
            operation_type=operation_type,
            delete_source=delete_source,
            status="pending",
            retry_count=0,
            created_at=timestamp,
            updated_at=timestamp,
        )
        
        db.add(task)
        logger.info(f"【队列处理】创建任务: {file_name} | 类型: {operation_type}")
        
        return task

    @staticmethod
    @db_query
    def count_by_status(db: Session) -> Dict[str, int]:
        """
        统计各状态任务数量
        
        用于监控和展示队列状态
        
        :param db: 数据库会话
        :return: 状态统计字典 {"pending": 5, "processing": 1, ...}
        """
        results = db.execute(
            select(UploadQueue.status, func.count())
            .group_by(UploadQueue.status)
        ).all()
        
        stats = {status: count for status, count in results}
        
        # 确保所有状态都有值（即使为0）
        for status in ["pending", "processing", "completed", "failed"]:
            if status not in stats:
                stats[status] = 0
        
        return stats

    @staticmethod
    @db_update
    def reset_processing_tasks(db: Session) -> int:
        """
        重置所有处理中的任务为待处理
        
        在启动时调用，处理上次异常退出的情况
        
        :param db: 数据库会话
        :return: 重置的任务数量
        """
        timestamp = int(datetime.now().timestamp())
        
        result = db.execute(
            update(UploadQueue)
            .where(UploadQueue.status == "processing")
            .values(status="pending", updated_at=timestamp)
        )
        
        count = result.rowcount
        if count > 0:
            logger.info(f"【队列处理器】重置了 {count} 个僵尸任务")
        
        return count

    @staticmethod
    @db_query
    def get_failed_tasks(db: Session, limit: int = 10) -> List["UploadQueue"]:
        """
        获取失败的任务列表
        
        :param db: 数据库会话
        :param limit: 返回数量限制
        :return: 失败任务列表
        """
        tasks = db.execute(
            select(UploadQueue)
            .where(UploadQueue.status == "failed")
            .order_by(UploadQueue.updated_at.desc())
            .limit(limit)
        ).scalars().all()
        
        return list(tasks)

