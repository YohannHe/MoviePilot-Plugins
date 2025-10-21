from typing import Optional
from datetime import datetime

from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    BigInteger,
    select,
)
from sqlalchemy.orm import Session

from app.log import logger
from ...db_manager import db_update, db_query, P115StrmHelperBase


class UploadHistory(P115StrmHelperBase):
    """
    文件上传历史记录表
    
    用于去重检查，避免相同文件重复上传。
    基于文件指纹（大小+修改时间）判断文件是否已上传过。
    
    文件指纹格式："{file_size}_{file_mtime}"
    - 轻量级：无需计算 SHA256，不消耗网络流量（适合 rclone 场景）
    - 准确性：对于正常场景足够准确
    """

    __tablename__ = "upload_history"

    # 主键
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # 文件指纹（用于去重）
    file_fingerprint = Column(String(100), nullable=False, index=True)  # 格式: "{size}_{mtime}"
    file_size = Column(BigInteger, nullable=False)  # 文件大小（字节，用于双重验证）
    
    # 文件信息
    file_path = Column(Text, nullable=False)  # 原始文件路径
    file_name = Column(String(255), nullable=False)  # 文件名
    
    # 目标信息
    dest_remote = Column(Text)  # 上传到的网盘路径
    dest_local = Column(Text)  # 复制到的本地路径
    operation_type = Column(String(20), nullable=False)  # "upload" 或 "copy"
    
    # 时间戳
    completed_at = Column(BigInteger, nullable=False, index=True)  # 完成时间戳

    @staticmethod
    @db_query
    def is_uploaded(db: Session, fingerprint: str, file_size: int) -> bool:
        """
        检查文件是否已上传过
        
        基于文件指纹和大小双重验证
        
        :param db: 数据库会话
        :param fingerprint: 文件指纹（格式: "{size}_{mtime}"）
        :param file_size: 文件大小（字节）
        :return: True 如果已上传过，否则 False
        """
        logger.debug(f"【去重检查】检查文件指纹: {fingerprint[:30]}... | 大小: {file_size / 1024 / 1024:.2f}MB")
        
        count = db.execute(
            select(UploadHistory)
            .where(
                UploadHistory.file_fingerprint == fingerprint,
                UploadHistory.file_size == file_size
            )
        ).scalar()
        
        exists = count is not None
        
        if exists:
            logger.info(f"【去重检查】文件已上传过（历史记录匹配）: {fingerprint}")
        
        return exists

    @staticmethod
    @db_update
    def record_upload(
        db: Session,
        fingerprint: str,
        file_size: int,
        file_path: str,
        file_name: str,
        operation_type: str,
        dest_remote: Optional[str] = None,
        dest_local: Optional[str] = None,
    ):
        """
        记录成功的上传
        
        :param db: 数据库会话
        :param fingerprint: 文件指纹
        :param file_size: 文件大小
        :param file_path: 文件路径
        :param file_name: 文件名
        :param operation_type: 操作类型
        :param dest_remote: 网盘目标路径
        :param dest_local: 本地目标路径
        """
        timestamp = int(datetime.now().timestamp())
        
        record = UploadHistory(
            file_fingerprint=fingerprint,
            file_size=file_size,
            file_path=file_path,
            file_name=file_name,
            dest_remote=dest_remote,
            dest_local=dest_local,
            operation_type=operation_type,
            completed_at=timestamp,
        )
        
        db.add(record)
        logger.info(
            f"【历史记录】记录上传成功: {file_name} | "
            f"类型: {operation_type} | "
            f"大小: {file_size / 1024 / 1024:.2f}MB"
        )

    @staticmethod
    @db_update
    def cleanup_old_records(db: Session, days: int = 90) -> int:
        """
        清理N天前的旧记录
        
        定期清理历史记录，避免数据库膨胀
        
        :param db: 数据库会话
        :param days: 保留天数（默认90天）
        :return: 清理的记录数
        """
        cutoff_timestamp = int(datetime.now().timestamp() - days * 86400)
        
        from sqlalchemy import delete
        
        result = db.execute(
            delete(UploadHistory).where(UploadHistory.completed_at < cutoff_timestamp)
        )
        
        count = result.rowcount
        
        if count > 0:
            logger.info(f"【历史清理】清理了 {count} 条 {days} 天前的记录")
        else:
            logger.debug(f"【历史清理】无需清理（{days} 天内无旧记录）")
        
        return count

    @staticmethod
    @db_query
    def get_recent_uploads(db: Session, limit: int = 20):
        """
        获取最近的上传记录
        
        用于展示和调试
        
        :param db: 数据库会话
        :param limit: 返回数量限制
        :return: 上传记录列表
        """
        records = db.execute(
            select(UploadHistory)
            .order_by(UploadHistory.completed_at.desc())
            .limit(limit)
        ).scalars().all()
        
        return list(records)

