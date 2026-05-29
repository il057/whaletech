import os
from dotenv import load_dotenv
# 在任何其他模块导入和环境变量读取前，最先加载 .env
load_dotenv()

import uuid as uuid_lib
import uvicorn
import asyncio
import aiosqlite
import logging
import json
import re
from datetime import datetime
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from openai import AsyncOpenAI

from database import DB_FILE, init_db
from voice_pipeline import VoicePipeline
from validator import validate_visitor_data
from wechat_bot import (
    send_visitor_notification,
    ensure_session,
    get_updates,
    send_text_message,
    send_typing_indicator,
    update_user_context_token,
    DEFAULT_BASE_URL
)

# 每个用户的对话历史，key = from_user_id，value = [{role, content}, ...]
# 保留最近 MAX_HISTORY_TURNS 轮（每轮 = user + assistant 各一条）
_user_histories: dict = {}
MAX_HISTORY_TURNS = 5

def _get_history(user_id: str) -> list:
    return _user_histories.get(user_id, [])

def _append_history(user_id: str, user_msg: str, assistant_msg: str):
    hist = _user_histories.setdefault(user_id, [])
    hist.append({"role": "user",      "content": user_msg})
    hist.append({"role": "assistant", "content": assistant_msg})
    # 超出上限时裁剪最早的
    if len(hist) > MAX_HISTORY_TURNS * 2:
        _user_histories[user_id] = hist[-(MAX_HISTORY_TURNS * 2):]

# ---------------------------------------------------------------------------
# SQL 安全拦截：仅允许 SELECT / WITH(CTE) 语句，拦截任何写操作
# ---------------------------------------------------------------------------
_SQL_DANGEROUS = re.compile(
    r'\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|REPLACE|TRUNCATE|ATTACH|DETACH|PRAGMA|VACUUM)\b',
    re.IGNORECASE
)

def _is_safe_select(stmt: str) -> bool:
    """返回 True 表示语句安全（纯 SELECT/WITH），否则应被拦截。"""
    # 剥离单行注释与块注释，防止注释绕过
    clean = re.sub(r'--[^\n]*', '', stmt)
    clean = re.sub(r'/\*.*?\*/', '', clean, flags=re.DOTALL)
    clean = clean.strip()
    # 必须以 SELECT 或 WITH（CTE）开头
    if not re.match(r'^(SELECT|WITH)\b', clean, re.IGNORECASE):
        return False
    # 二次检查：即便以 SELECT 开头，子查询中也不得含危险关键字
    if _SQL_DANGEROUS.search(clean):
        return False
    return True


async def register_visitor_from_wechat(name: str, phone: str, plate: str, company: str, reason: str, visit_time: str = None) -> str:
    """
    由保安通过微信手动补录访客信息：
    - 按手机号或车牌匹配已有用户，否则创建新用户
    - 更新用户档案中的空白字段
    - 将最近一条匹配的 pending_human_cases 标记为 resolved
    - 新增 visits 记录（时间取门卫提供的时间，否则取消息处理时刻）
    - 返回确认文字
    """
    # -----------------------------------------------------------------------
    # 数据清洗拦截层：校验 phone / plate 合法性，不合规则直接拒绝入库
    # -----------------------------------------------------------------------
    is_valid, failed_field, correction_prompt = validate_visitor_data(
        {"phone": phone or "", "plate": plate or ""}
    )
    if not is_valid:
        logging.warning(
            f'[微信补录-数据校验] 拦截入库，字段 [{failed_field}] 不合规 — '
            f'phone="{phone}" plate="{plate}"'
        )
        # 直接把纠错指令作为回复，让大模型接管后续修正流程
        return f"❌ 登记被拦截\n{correction_prompt}"
    # -----------------------------------------------------------------------
    try:
        async with aiosqlite.connect(DB_FILE) as conn:
            conn.row_factory = aiosqlite.Row

            # 1. 按手机号或车牌匹配现有用户
            user_uuid = None
            if phone:
                async with conn.execute("SELECT uuid FROM users WHERE phone = ?", (phone,)) as cur:
                    row = await cur.fetchone()
                    if row:
                        user_uuid = row["uuid"]
            if not user_uuid and plate:
                async with conn.execute("SELECT uuid FROM users WHERE default_plate = ?", (plate,)) as cur:
                    row = await cur.fetchone()
                    if row:
                        user_uuid = row["uuid"]

            # 2. 没有匹配到则创建新用户
            if not user_uuid:
                user_uuid = str(uuid_lib.uuid4())
                await conn.execute("INSERT OR IGNORE INTO users (uuid) VALUES (?)", (user_uuid,))

            # 3. 只更新有值的字段，不覆盖已有信息
            if phone:
                await conn.execute("UPDATE users SET phone=? WHERE uuid=?", (phone, user_uuid))
            if name:
                await conn.execute("UPDATE users SET name=? WHERE uuid=?", (name, user_uuid))
            if plate:
                await conn.execute("UPDATE users SET default_plate=? WHERE uuid=?", (plate, user_uuid))
            if company:
                await conn.execute("UPDATE users SET default_company=? WHERE uuid=?", (company, user_uuid))

            # 4. 新增来访记录（时间优先取 LLM 从消息中解析的时间，兜底为当前时间）
            actual_time = visit_time if visit_time else datetime.now().strftime('%Y/%m/%d %H:%M')
            visit_reason_str = reason if reason else (company or "人工登记")
            await conn.execute(
                "INSERT INTO visits (user_uuid, visit_reason, timestamp) VALUES (?, ?, ?)",
                (user_uuid, visit_reason_str, actual_time)
            )

            # 5. 将最近一条匹配的 pending 案件标记为 resolved
            match_params = []
            match_cond = []
            if plate:
                match_cond.append("partial_plate=?")
                match_params.append(plate)
            if phone:
                match_cond.append("partial_phone=?")
                match_params.append(phone)
            if match_cond:
                where = " OR ".join(match_cond)
                await conn.execute(f"""
                    UPDATE pending_human_cases SET status='resolved'
                    WHERE id = (
                        SELECT id FROM pending_human_cases
                        WHERE status='pending' AND ({where})
                        ORDER BY created_at DESC LIMIT 1
                    )
                """, match_params)

            await conn.commit()

        # 6. 构造确认消息
        parts = []
        if name:    parts.append(f"姓名: {name}")
        if plate:   parts.append(f"车牌: {plate}")
        if phone:   parts.append(f"电话: {phone}")
        if company: parts.append(f"单位: {company}")
        if reason:  parts.append(f"事由: {reason}")
        parts.append(f"时间: {actual_time}")
        logging.info(f"[微信补录] 访客 {name or plate or phone} 信息已入库，时间: {actual_time}")
        return "✅ 访客信息已登记\n" + "\n".join(parts)

    except Exception as e:
        logging.error(f"微信补录入库失败: {e}")
        return "❌ 登记失败，请检查信息格式后重试。"



# 保活短语集合：保安发送这些词时仅刷新 context_token，不触发 LLM
# 目的：维持 iLink 会话窗口活跃，避免超过约 10 条主动推送的投递上限
_KEEPALIVE_PHRASES = {
    # 确认/收悉
    "收到", "知道了", "了解", "明白", "好的", "好", "ok", "OK", "Ok",
    "嗯", "嗯嗯", "哦", "哦哦", "是", "是的", "对", "行",
    # 感谢/礼貌
    "谢谢", "谢", "辛苦了", "辛苦", "好的谢谢",
    # 数字/符号确认
    "1", "。", ".",
}

async def handle_wechat_message(base_url, token, to_user_id, message, context_token, openai_client):
    """
    处理保安通过微信发来的消息，支持三种意图：
    A. 补充登记访客信息（手动补录）
    B. 查询数据库（Text-to-SQL，支持多条语句）
    C. 其他一般对话
    携带最近 MAX_HISTORY_TURNS 轮上下文，让 AI 理解追问。
    """
    # 保活短语：仅刷新 context_token，不消耗 LLM token
    if message.strip() in _KEEPALIVE_PHRASES:
        logging.info(f"[保活] 收到确认短语 '{message.strip()}'，已刷新会话窗口，跳过 LLM")
        return

    db_schema = (
        "数据库表结构（SQL中字段名必须与此完全一致，不得自行更改）:\n"
        "TABLE users (uuid TEXT, name TEXT, phone TEXT, default_plate TEXT, default_company TEXT)\n"
        "TABLE visits (id INTEGER, user_uuid TEXT, visit_reason TEXT, timestamp DATETIME)\n"
        "  -- visits 表的时间列名是 timestamp，不是 visit_time、time 或其他名称\n"
        "  -- 按日期查询示例: WHERE DATE(timestamp) = '2026-05-29'\n"
        "  -- 按小时分布示例: strftime('%H', timestamp)\n"
        "TABLE pending_human_cases (id INTEGER, user_uuid TEXT, partial_name TEXT, partial_phone TEXT, "
        "partial_plate TEXT, partial_company TEXT, partial_reason TEXT, trigger_reason TEXT, status TEXT, created_at DATETIME)\n"
    )

    system_prompt = f"""你是一个智能门卫数据助手。
{db_schema}
当前时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

判断保安发来的消息类型并作出对应处理：

【类型A：补充登记访客信息】
如果保安是在补录某位访客的信息（例如"刚刚那个车牌是xxx手机xxx去xx公司送货"、"帮我录一下沪A12345，手机138xxxx，去华为送货"、"补一条：张三，沪B99999，去阿里拜访"），
请仅输出以下格式，不要输出任何其他内容：
===REGISTER_BEGIN===
{{"name":"","phone":"","plate":"","company":"","reason":"","visit_time":""}}
===REGISTER_END===
字段说明：name(姓名，可为空), phone(手机号，可为空), plate(车牌号), company(来访单位), reason(事由动作如送货/拜访/施工), visit_time(来访时间，若门卫明确提到时间则填写格式 YYYY-MM-DD HH:MM:SS，否则留空字符串)

【类型B：查询或分析数据库】
如果保安是在查询统计或分析记录（例如"今天来了几辆车"、"全面分析来访数据"、"找一下沪A12345的记录"、"有哪些待处理案件"），
请写出能在SQLite执行的合法SQL查询语句，包在<sql>和</sql>之间。
可以包含多条SELECT语句，用分号分隔，系统会逐条执行并汇总结果。
重要：visits表的时间列名是 timestamp（不是 visit_time），查询时必须用 timestamp，例如 DATE(timestamp) = '2026-05-29'。

【类型C：其他对话】
直接用自然语言回答，不包含以上任何格式标记。"""

    # 先异步发送"正在输入"状态
    asyncio.create_task(send_typing_indicator(base_url, token, to_user_id, context_token))

    # 拼装带历史上下文的消息列表
    history = _get_history(to_user_id)
    messages = [{"role": "system", "content": system_prompt}]
    messages.extend(history)
    messages.append({"role": "user", "content": message})

    try:
        resp1 = await openai_client.chat.completions.create(
            model="deepseek-v4-flash-260425",
            messages=messages
        )
        reply1 = resp1.choices[0].message.content

        # 类型A：访客登记
        if "===REGISTER_BEGIN===" in reply1 and "===REGISTER_END===" in reply1:
            start = reply1.find("===REGISTER_BEGIN===") + len("===REGISTER_BEGIN===")
            end = reply1.find("===REGISTER_END===")
            json_str = reply1[start:end].strip()
            data = json.loads(json_str)
            confirm_msg = await register_visitor_from_wechat(
                data.get("name", ""),
                data.get("phone", ""),
                data.get("plate", ""),
                data.get("company", ""),
                data.get("reason", ""),
                data.get("visit_time", "") or None
            )
            await send_text_message(base_url, token, to_user_id, confirm_msg, context_token)
            _append_history(to_user_id, message, confirm_msg)
            return

        # 类型B：SQL 查询——支持多条语句，逐条执行并汇总结果
        sql_match = re.search(r'<sql>(.*?)</sql>', reply1, re.IGNORECASE | re.DOTALL)
        if sql_match:
            raw_sql = sql_match.group(1).strip()
            # 按分号拆分，过滤空语句
            statements = [s.strip() for s in raw_sql.split(';') if s.strip()]
            all_results = []
            async with aiosqlite.connect(DB_FILE) as conn:
                for stmt in statements:
                    # ---- SQL 安全拦截：仅允许只读 SELECT 语句 ----
                    if not _is_safe_select(stmt):
                        logging.warning(f"[SQL 安全拦截] 非 SELECT 语句已被阻止: {stmt[:120]}")
                        all_results.append({
                            "sql": stmt,
                            "error": "⚠️ 该语句已被安全策略拦截，仅允许执行 SELECT 查询。"
                        })
                        continue
                    try:
                        async with conn.execute(stmt) as cur:
                            rows = await cur.fetchall()
                            all_results.append({"sql": stmt, "rows": [list(r) for r in rows]})
                    except Exception as sql_err:
                        all_results.append({"sql": stmt, "error": str(sql_err)})
                        logging.warning(f"SQL 子语句执行失败: {stmt} — {sql_err}")

            resp2 = await openai_client.chat.completions.create(
                model="deepseek-v4-flash-260425",
                messages=[
                    {"role": "system", "content": system_prompt},
                    *history,
                    {"role": "user", "content": message},
                    {"role": "assistant", "content": reply1},
                    {"role": "user", "content": f"各条SQL执行结果如下：{all_results}\n请用自然语言向保安简短精准地汇报分析结论，不许出现任何SQL语句。"}
                ]
            )
            final_reply = resp2.choices[0].message.content
        else:
            # 类型C：直接回答
            final_reply = reply1

        await send_text_message(base_url, token, to_user_id, final_reply, context_token)
        _append_history(to_user_id, message, final_reply)

    except Exception as e:
        logging.error(f"处理保安微信消息时出错: {e}")
        await send_text_message(base_url, token, to_user_id, "抱歉，处理消息时遇到系统异常，请稍后重试。", context_token)

async def wechat_long_polling_loop():
    """
    长轮询循环任务，用来持续获取原生协议机器人收到的所有微信消息
    """
    sync_buf = ""
    # 初始化火山引擎大模型客户端
    api_key = os.getenv('ARK_API_KEY')
    if api_key:
        client = AsyncOpenAI(
            base_url='https://ark.cn-beijing.volces.com/api/v3',
            api_key=api_key
        )
    else:
        logging.warning("ARK_API_KEY 未设置，保安自然语言查询功能受限")
        client = None
    
    # 优先在控制台完成登录（阻塞直到完成扫码并返回 session）
    await ensure_session()
    logging.info("微信官方机器人长轮询后台任务启动...")

    while True:
        try:
            # 持续加载最新 session，确保如 Token 等变更后立即生效
            session = await ensure_session()
            base_url = session.get("baseUrl", DEFAULT_BASE_URL)
            token = session.get("token")
            
            updates = await get_updates(base_url, token, sync_buf)
            sync_buf = updates.get("get_updates_buf", sync_buf)
            
            # 遍历所有新收到的消息
            for msg in updates.get("msgs", []):
                msg_type = msg.get("message_type")
                context_token = msg.get("context_token")

                if msg_type == 2:
                    # bot 自身消息的 echo（服务端回显）：to_user_id 才是真正的对话用户
                    # 捕获 echo 中的 context_token 以便滚动刷新（若服务端下发了新 token）
                    echo_user = msg.get("to_user_id")
                    if echo_user and context_token:
                        update_user_context_token(echo_user, context_token)
                    continue  # echo 消息不触发业务处理

                if msg_type != 1:
                    continue  # 其他未知消息类型，跳过

                from_user_id = msg.get("from_user_id")

                # 缓存该用户最新的 context_token，供主动推送时复用
                # iLink 协议要求发送消息时必须携带合法的 context_token，
                # 否则超过约 10 条主动推送后服务端将停止投递
                if from_user_id and context_token:
                    update_user_context_token(from_user_id, context_token)
                
                for item in msg.get("item_list", []):
                    # 类型是普通文本信息
                    if item.get("type") == 1 and item.get("text_item"): 
                        question = item["text_item"].get("text", "").strip()
                        if question and from_user_id and client:
                            logging.info(f"[门卫消息] 收到: {question}")
                            await handle_wechat_message(base_url, token, from_user_id, question, context_token, client)
            
            await asyncio.sleep(0.5)
        except Exception as e:
            logging.error(f"长轮询轮次异常 (可能被降级或失联): {e}")
            await asyncio.sleep(2)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 确保数据库与最新表结构同步（幂等操作）
    await init_db()
    # 系统启动时，孵化常驻的微信协议轮询协程
    wechat_task = asyncio.create_task(wechat_long_polling_loop())
    yield
    # 系统停止时取消后台任务
    wechat_task.cancel()

app = FastAPI(lifespan=lifespan)

# 挂载静态文件目录，用于服务 index.html 和其他前端资源
app.mount("/static", StaticFiles(directory="static"), name="static")

class QuickPassRequest(BaseModel):
    user_uuid: str
    name: str
    phone: str
    plate: str
    company: str
    reason: str

@app.get("/api/visitor/{user_uuid}")
async def get_visitor_info(user_uuid: str):
    """
    提供给前端，查询是否是老访客，以决定是否展示一键登记按钮
    """
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT * FROM users WHERE uuid = ?", (user_uuid,)) as cur:
            user = await cur.fetchone()

        if not user or not user["phone"]:
            return {"is_old": False}

        async with conn.execute(
            "SELECT visit_reason FROM visits WHERE user_uuid = ? ORDER BY timestamp DESC LIMIT 1",
            (user_uuid,)
        ) as cur:
            last_visit = await cur.fetchone()

    reason = last_visit["visit_reason"] if last_visit else "办事"
    return {
        "is_old": True,
        "name": user["name"] or "",
        "phone": user["phone"] or "",
        "plate": user["default_plate"] or "",
        "company": user["default_company"] or "",
        "reason": reason
    }

@app.post("/api/visitor/quick_pass")
async def quick_pass(req: QuickPassRequest):
    """
    处理一键放行的请求，绕过语音AI直接入库并通知门卫
    """
    async with aiosqlite.connect(DB_FILE) as conn:
        await conn.execute(
            "INSERT INTO visits (user_uuid, visit_reason) VALUES (?, ?)",
            (req.user_uuid, req.reason)
        )
        await conn.commit()

        # 统计本月来访次数
        async with conn.execute(
            "SELECT COUNT(*) FROM visits WHERE user_uuid = ? AND strftime('%Y-%m', timestamp) = strftime('%Y-%m', 'now')",
            (req.user_uuid,)
        ) as cur:
            row = await cur.fetchone()
            month_count = row[0]

    notice_msg = f"提示: 该访客本月已来访 {month_count} 次"
    await send_visitor_notification(req.name, req.plate, req.phone, req.company, req.reason, notice_msg)

    return {"status": "success"}

@app.get("/", response_class=HTMLResponse)
async def get_index():
    """
    返回极简的前端页面。
    实际业务中，访客可以通过扫码门卫室的二维码直接打开这个落地页。
    """
    with open("static/index.html", "r", encoding="utf-8") as f:
        return f.read()

@app.websocket("/ws/voice/{user_uuid}")
async def websocket_voice_endpoint(websocket: WebSocket, user_uuid: str):
    """
    处理浏览器发起的 WebSocket 语音通话请求。
    将核心的信令、音频收发丢给 VoicePipeline 去处理。
    """
    pipeline = VoicePipeline(user_uuid)
    await pipeline.connect_and_handle(websocket)

if __name__ == "__main__":
    # 使用 Uvicorn 启动 ASGI 服务器
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
