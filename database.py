import aiosqlite
import asyncio
import logging
from pathlib import Path

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_FILE = Path(__file__).parent / "data.db"

async def init_db():
    """
    初始化 SQLite 数据库文件并创建数据表（如果不存在）。
    使用 aiosqlite 进行全异步操作，不阻塞事件循环。
    """
    try:
        async with aiosqlite.connect(DB_FILE) as conn:
            # 1. 创建访客用户表 (users)
            # 记录访客的长期固有属性
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    uuid TEXT PRIMARY KEY,       -- 前端生成的唯一标识
                    phone TEXT,                  -- 手机号
                    name TEXT,                   -- 姓名/称呼
                    default_plate TEXT,          -- 默认车牌号
                    default_company TEXT         -- 常去单位
                )
            ''')

            # 2. 创建来访记录表 (visits)
            # 记录单次来访的事件信息，含当次快照字段，避免用户档案更新后历史记录被覆写
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS visits (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_uuid TEXT,              -- 关联的访客UUID
                    name TEXT,                   -- 当次来访姓名快照
                    phone TEXT,                  -- 当次来访手机快照
                    plate TEXT,                  -- 当次来访车牌快照
                    company TEXT,                -- 当次来访单位快照
                    visit_reason TEXT,           -- 来访事由
                    timestamp DATETIME DEFAULT (datetime('now', 'localtime')), -- 记录创建时间（本地时间）
                    FOREIGN KEY (user_uuid) REFERENCES users (uuid)
                )
            ''')

            # 向前兼容迁移：为已存在的旧 visits 表补充快照字段
            async with conn.execute("PRAGMA table_info(visits)") as cur:
                existing_cols = {row[1] async for row in cur}
            for col in ["name", "phone", "plate", "company"]:
                if col not in existing_cols:
                    await conn.execute(f"ALTER TABLE visits ADD COLUMN {col} TEXT")
                    logging.info(f"数据库迁移：visits 表新增列 {col}")

            # 3. 创建人工待处理记录表 (pending_human_cases)
            # 记录 AI 无法自动完成登记、需要人工介入的来访会话
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS pending_human_cases (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_uuid TEXT,              -- 关联的访客UUID（可为空）
                    partial_name TEXT,           -- 已采集的姓名（可能不完整）
                    partial_phone TEXT,          -- 已采集的手机号
                    partial_plate TEXT,          -- 已采集的车牌号
                    partial_company TEXT,        -- 已采集的来访单位
                    partial_reason TEXT,         -- 已采集的事由
                    trigger_reason TEXT,         -- 触发人工的原因
                    status TEXT DEFAULT 'pending', -- 状态：pending / resolved
                    created_at DATETIME DEFAULT (datetime('now', 'localtime')),
                    FOREIGN KEY (user_uuid) REFERENCES users (uuid)
                )
            ''')

            await conn.commit()
        logging.info(f"数据库初始化成功: {DB_FILE}")

    except Exception as e:
        logging.error(f"数据库初始化失败: {e}")

if __name__ == "__main__":
    # 单独运行此文件时，执行建表操作
    asyncio.run(init_db())
