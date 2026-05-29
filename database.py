import sqlite3
import logging
from pathlib import Path

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_FILE = Path(__file__).parent / "data.db"

def init_db():
    """
    初始化 SQLite 数据库文件并创建数据表（如果不存在）。
    使用纯 sqlite3，无需 ORM。
    """
    try:
        # 连接数据库（文件不存在时会自动创建）
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        # 1. 创建访客用户表 (users)
        # 记录访客的长期固有属性
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                uuid TEXT PRIMARY KEY,       -- 前端生成的唯一标识
                phone TEXT,                  -- 手机号
                name TEXT,                   -- 姓名/称呼
                default_plate TEXT,          -- 默认车牌号
                default_company TEXT         -- 常去单位
            )
        ''')

        # 2. 创建来访记录表 (visits)
        # 记录单次来访的事件信息
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS visits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_uuid TEXT,              -- 关联的访客UUID
                visit_reason TEXT,           -- 来访事由
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, -- 记录创建时间
                FOREIGN KEY (user_uuid) REFERENCES users (uuid)
            )
        ''')

        # 3. 创建人工待处理记录表 (pending_human_cases)
        # 记录 AI 无法自动完成登记、需要人工介入的来访会话
        cursor.execute('''
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
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_uuid) REFERENCES users (uuid)
            )
        ''')

        conn.commit()
        logging.info(f"数据库初始化成功: {DB_FILE}")
        
    except sqlite3.Error as e:
        logging.error(f"数据库初始化失败: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    # 单独运行此文件时，执行建表操作
    init_db()
