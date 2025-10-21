"""1.0.2 - 添加目录上传队列化支持

Revision ID: add_upload_queue
Revises: 2606909750bf
Create Date: 2025-10-21 00:00:00.000000

添加两张新表：
- upload_queue: 任务队列表，用于持久化待处理的上传任务
- upload_history: 历史记录表，用于去重检查

"""

from alembic import op
import sqlalchemy as sa

from app.log import logger


# revision identifiers, used by Alembic.
version = '1.0.2'
revision = "add_upload_queue"
down_revision = "2606909750bf"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """创建上传队列和历史表"""
    
    logger.info("【数据库迁移】开始创建上传队列表...")
    
    # 创建 upload_queue 表
    op.create_table(
        'upload_queue',
        sa.Column('id', sa.Integer(), nullable=False, primary_key=True, autoincrement=True),
        sa.Column('file_path', sa.Text(), nullable=False),
        sa.Column('file_name', sa.String(255), nullable=False),
        sa.Column('file_size', sa.BigInteger(), nullable=False),
        sa.Column('file_mtime', sa.BigInteger(), nullable=False),
        sa.Column('monitor_path', sa.Text(), nullable=False),
        sa.Column('dest_remote', sa.Text(), nullable=True),
        sa.Column('dest_local', sa.Text(), nullable=True),
        sa.Column('operation_type', sa.String(20), nullable=False),
        sa.Column('delete_source', sa.Boolean(), nullable=False, server_default='0'),
        sa.Column('status', sa.String(20), nullable=False, server_default='pending'),
        sa.Column('retry_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('created_at', sa.BigInteger(), nullable=False),
        sa.Column('updated_at', sa.BigInteger(), nullable=False),
    )
    
    # 创建 upload_queue 索引
    op.create_index('ix_upload_queue_file_path', 'upload_queue', ['file_path'], unique=True)
    op.create_index('ix_upload_queue_status', 'upload_queue', ['status'])
    
    logger.info("【数据库迁移】upload_queue 表创建完成")
    
    # 创建 upload_history 表
    logger.info("【数据库迁移】开始创建上传历史表...")
    
    op.create_table(
        'upload_history',
        sa.Column('id', sa.Integer(), nullable=False, primary_key=True, autoincrement=True),
        sa.Column('file_fingerprint', sa.String(100), nullable=False),
        sa.Column('file_size', sa.BigInteger(), nullable=False),
        sa.Column('file_path', sa.Text(), nullable=False),
        sa.Column('file_name', sa.String(255), nullable=False),
        sa.Column('dest_remote', sa.Text(), nullable=True),
        sa.Column('dest_local', sa.Text(), nullable=True),
        sa.Column('operation_type', sa.String(20), nullable=False),
        sa.Column('completed_at', sa.BigInteger(), nullable=False),
    )
    
    # 创建 upload_history 索引
    op.create_index('ix_upload_history_fingerprint', 'upload_history', ['file_fingerprint'])
    op.create_index('ix_upload_history_fingerprint_size', 'upload_history', ['file_fingerprint', 'file_size'])
    op.create_index('ix_upload_history_completed_at', 'upload_history', ['completed_at'])
    
    logger.info("【数据库迁移】upload_history 表创建完成")
    logger.info("【数据库迁移】上传队列化功能数据库升级完成")


def downgrade() -> None:
    """删除上传队列和历史表"""
    
    logger.info("【数据库迁移】开始回滚上传队列表...")
    
    # 删除 upload_history 索引和表
    op.drop_index('ix_upload_history_completed_at', table_name='upload_history')
    op.drop_index('ix_upload_history_fingerprint_size', table_name='upload_history')
    op.drop_index('ix_upload_history_fingerprint', table_name='upload_history')
    op.drop_table('upload_history')
    
    logger.info("【数据库迁移】upload_history 表已删除")
    
    # 删除 upload_queue 索引和表
    op.drop_index('ix_upload_queue_status', table_name='upload_queue')
    op.drop_index('ix_upload_queue_file_path', table_name='upload_queue')
    op.drop_table('upload_queue')
    
    logger.info("【数据库迁移】upload_queue 表已删除")
    logger.info("【数据库迁移】上传队列化功能数据库回滚完成")

