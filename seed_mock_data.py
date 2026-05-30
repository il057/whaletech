"""
seed_mock_data.py
生成用于演示的 SQLite mock 数据。
运行方式：python seed_mock_data.py
"""

import asyncio
import uuid
import random
from datetime import datetime, timedelta
from pathlib import Path
import aiosqlite

DB_FILE = Path(__file__).parent / "data.db"

# ── Mock 数据素材 ──────────────────────────────────────────────────────────────

NAMES = [
    "张伟", "李娜", "王芳", "赵刚", "刘洋", "陈军", "杨帆", "黄丽", "周鑫", "吴静",
    "徐明", "孙晓燕", "马建国", "朱海燕", "胡志远", "郑雪梅", "何天宇", "高燕", "林峰", "罗晨",
]

PHONE_PREFIXES = ["138", "139", "135", "136", "150", "151", "186", "187", "189", "177"]

PLATES = [
    "京A12345", "京B67890", "沪C11223", "粤B88521", "浙A33456",
    "苏E77889", "川A00112", "渝B55667", "闽D22334", "湘C44556",
    "豫A99001", "鄂B31245", "皖C65432", "赣A10293", "黑B84756",
    "吉A37284", "辽C49382", "桂B12837", "云A73829", "贵C83721",
]

COMPANIES = [
    "顺丰速运", "京东物流", "中通快递", "圆通速递", "申通快递",
    "华为技术", "腾讯科技", "阿里巴巴", "小米科技", "字节跳动",
    "中国建筑", "中国电建", "万科集团", "恒大集团", "碧桂园",
    "平安保险", "招商银行", "工商银行", "中国移动", "中国联通",
    "个人访客", "政府单位", "高校访客",
]

REASONS = [
    "快递取件", "快递送件", "业务洽谈", "工程维修", "设备巡检",
    "访问员工", "参加会议", "面试求职", "签署合同", "物业报修",
    "验收工程", "安装调试", "参观考察", "培训学习", "送餐配送",
    "水电检修", "消防检查", "电梯维保", "网络维护", "空调维修",
]

TRIGGER_REASONS = [
    "访客语音识别置信度低，需人工确认信息",
    "来访事由描述异常，疑似非常规访客",
    "手机号格式无法识别，需人工录入",
    "车牌号未能从对话中提取",
    "访客拒绝提供姓名，需人工核实身份",
    "多次识别失败，AI 无法完成登记",
    "访客声称预约但系统无记录，需核实",
]


def rand_phone() -> str:
    return random.choice(PHONE_PREFIXES) + "".join([str(random.randint(0, 9)) for _ in range(8)])


def rand_dt(days_ago_min: int, days_ago_max: int) -> str:
    """在过去 days_ago_min ~ days_ago_max 天之间随机生成一个工作时段时间戳。"""
    delta = random.randint(days_ago_min, days_ago_max)
    base = datetime.now() - timedelta(days=delta)
    # 8:30 ~ 18:30 工作时段
    hour = random.randint(8, 18)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return base.replace(hour=hour, minute=minute, second=second).strftime("%Y-%m-%d %H:%M:%S")


# ── 建表（复用 database.py 逻辑，避免 import 依赖）──────────────────────────

CREATE_USERS = """
CREATE TABLE IF NOT EXISTS users (
    uuid TEXT PRIMARY KEY,
    phone TEXT,
    name TEXT,
    default_plate TEXT,
    default_company TEXT
)
"""

CREATE_VISITS = """
CREATE TABLE IF NOT EXISTS visits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_uuid TEXT,
    name TEXT,
    phone TEXT,
    plate TEXT,
    company TEXT,
    visit_reason TEXT,
    timestamp DATETIME DEFAULT (datetime('now','localtime')),
    FOREIGN KEY (user_uuid) REFERENCES users (uuid)
)
"""

CREATE_PENDING = """
CREATE TABLE IF NOT EXISTS pending_human_cases (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_uuid TEXT,
    partial_name TEXT,
    partial_phone TEXT,
    partial_plate TEXT,
    partial_company TEXT,
    partial_reason TEXT,
    trigger_reason TEXT,
    status TEXT DEFAULT 'pending',
    created_at DATETIME DEFAULT (datetime('now','localtime')),
    FOREIGN KEY (user_uuid) REFERENCES users (uuid)
)
"""


async def seed():
    print(f"目标数据库: {DB_FILE}")

    async with aiosqlite.connect(DB_FILE) as conn:
        # 建表
        await conn.execute(CREATE_USERS)
        await conn.execute(CREATE_VISITS)
        await conn.execute(CREATE_PENDING)
        await conn.commit()
        print("✓ 数据表已就绪")

        # ── 1. 清空旧 mock 数据 ──────────────────────────────────────────────
        await conn.execute("DELETE FROM visits")
        await conn.execute("DELETE FROM pending_human_cases")
        await conn.execute("DELETE FROM users")
        await conn.commit()
        print("✓ 旧数据已清空")

        # ── 2. 插入访客用户（20 人）────────────────────────────────────────────
        users = []
        for i, name in enumerate(NAMES):
            uid = str(uuid.uuid4())
            phone = rand_phone()
            plate = PLATES[i % len(PLATES)]
            company = random.choice(COMPANIES)
            users.append((uid, phone, name, plate, company))

        await conn.executemany(
            "INSERT INTO users (uuid, phone, name, default_plate, default_company) VALUES (?,?,?,?,?)",
            users,
        )
        await conn.commit()
        print(f"✓ 插入访客用户 {len(users)} 条")

        # ── 3. 插入来访记录 ────────────────────────────────────────────────────
        # 分布策略：今天 8 条 / 本月其余天 ~80 条 / 上月 ~60 条 / 上上月 ~30 条
        visits = []

        def add_visits(count: int, days_min: int, days_max: int):
            for _ in range(count):
                uid, phone, name, plate, company = random.choice(users)
                reason = random.choice(REASONS)
                ts = rand_dt(days_min, days_max)
                visits.append((uid, name, phone, plate, company, reason, ts))

        add_visits(8,   0,  0)    # 今天
        add_visits(80,  1, 29)    # 本月（近 30 天）
        add_visits(60, 30, 59)    # 上月
        add_visits(30, 60, 89)    # 上上月

        await conn.executemany(
            "INSERT INTO visits (user_uuid, name, phone, plate, company, visit_reason, timestamp) VALUES (?,?,?,?,?,?,?)",
            visits,
        )
        await conn.commit()
        print(f"✓ 插入来访记录 {len(visits)} 条")

        # ── 4. 插入人工待处理案件 ──────────────────────────────────────────────
        pending_rows = []

        # 5 条 pending（未处理）
        for _ in range(5):
            partial_name  = random.choice(NAMES) if random.random() > 0.3 else None
            partial_phone = rand_phone() if random.random() > 0.4 else None
            partial_plate = random.choice(PLATES) if random.random() > 0.5 else None
            partial_company = random.choice(COMPANIES) if random.random() > 0.4 else None
            partial_reason  = random.choice(REASONS) if random.random() > 0.3 else None
            trigger = random.choice(TRIGGER_REASONS)
            ts = rand_dt(0, 7)
            pending_rows.append((
                None, partial_name, partial_phone, partial_plate,
                partial_company, partial_reason, trigger, "pending", ts,
            ))

        # 5 条 resolved（已处理）
        for _ in range(5):
            uid, phone, name, plate, company = random.choice(users)
            reason = random.choice(REASONS)
            trigger = random.choice(TRIGGER_REASONS)
            ts = rand_dt(8, 60)
            pending_rows.append((
                uid, name, phone, plate, company, reason, trigger, "resolved", ts,
            ))

        await conn.executemany(
            """INSERT INTO pending_human_cases
               (user_uuid, partial_name, partial_phone, partial_plate,
                partial_company, partial_reason, trigger_reason, status, created_at)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            pending_rows,
        )
        await conn.commit()
        print(f"✓ 插入人工待处理案件 {len(pending_rows)} 条（5 pending + 5 resolved）")

    print("\n🎉 Mock 数据生成完毕！")
    print(f"   用户数：{len(users)}")
    print(f"   来访记录：{len(visits)}")
    print(f"   待处理案件：{len(pending_rows)}")
    print(f"\n数据库路径：{DB_FILE.resolve()}")


if __name__ == "__main__":
    asyncio.run(seed())
