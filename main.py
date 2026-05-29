import os
from dotenv import load_dotenv
# 在任何其他模块导入和环境变量读取前，最先加载 .env
load_dotenv()

import uvicorn
import asyncio
import sqlite3
import logging
import re
from datetime import datetime
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from openai import AsyncOpenAI

from database import DB_FILE
from voice_pipeline import VoicePipeline
from wechat_bot import (
    send_visitor_notification, 
    ensure_session, 
    get_updates, 
    send_text_message, 
    DEFAULT_BASE_URL
)

async def handle_llm_query(base_url, token, to_user_id, question, context_token, openai_client):
    """
    处理保安在微信上发出的自然语言查询（驱动 Text-to-SQL）
    """
    db_schema = (
        "数据库表结构: \n"
        "TABLE users (uuid TEXT, name TEXT, phone TEXT, default_plate TEXT, default_company TEXT)\n"
        "TABLE visits (id INTEGER, user_uuid TEXT, visit_reason TEXT, timestamp DATETIME)\n"
    )
    
    system_prompt = f"""你是一个智能门卫数据助手。
{db_schema}
当前时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
请你根据保安的问题，如果需要查询数据库，直接写出能在 SQLite 运行的合法 SQL 查询语句，包在 <sql> 和 </sql> 之间，不要返回其他分析内容，执行后我将给你结果，你再汇报给保安；如果不需要查库，直接给出回答，不可包含<sql>。"""

    try:
        # 第一轮：询问模型是否需要 SQL
        resp1 = await openai_client.chat.completions.create(
            model="deepseek-v4-flash-260425",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": question}
            ]
        )
        reply1 = resp1.choices[0].message.content
        
        # 判断并提取 SQL
        sql_match = re.search(r'<sql>(.*?)</sql>', reply1, re.IGNORECASE | re.DOTALL)
        if sql_match:
            sql = sql_match.group(1).strip()
            # 执行查询
            conn = sqlite3.connect(DB_FILE)
            cur = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            conn.close()
            
            # 第二轮：将 SQL 执行结果传给大模型，让它输出最终自然语言汇报
            resp2 = await openai_client.chat.completions.create(
                model="deepseek-v4-flash-260425",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": question},
                    {"role": "assistant", "content": reply1},
                    {"role": "user", "content": f"SQL 执行结果：{rows}，请用自然语言向保安简短、精准地汇报统计结果，不许出现任何SQL语句。"}
                ]
            )
            final_reply = resp2.choices[0].message.content
        else:
            final_reply = reply1
            
        await send_text_message(base_url, token, to_user_id, final_reply, context_token)
    except Exception as e:
        logging.error(f"处理保安自然语言查询时出错: {e}")
        error_msg = "抱歉，刚刚查询数据库时遇到系统异常，请稍后重试。"
        await send_text_message(base_url, token, to_user_id, error_msg, context_token)

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
                # 必须跳过机器人自己发出的消息（message_type == 2 代表 bot，1 代表 user）
                if msg.get("message_type") != 1:
                    continue
                
                context_token = msg.get("context_token")
                from_user_id = msg.get("from_user_id")
                
                for item in msg.get("item_list", []):
                    # 类型是普通文本信息
                    if item.get("type") == 1 and item.get("text_item"): 
                        question = item["text_item"].get("text", "").strip()
                        if question and from_user_id and client:
                            logging.info(f"[门卫自然询问] 收到消息: {question}")
                            await handle_llm_query(base_url, token, from_user_id, question, context_token, client)
            
            await asyncio.sleep(0.5)
        except Exception as e:
            logging.error(f"长轮询轮次异常 (可能被降级或失联): {e}")
            await asyncio.sleep(2)

@asynccontextmanager
async def lifespan(app: FastAPI):
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
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE uuid = ?", (user_uuid,))
    user = cur.fetchone()
    
    if not user or not user["phone"]:
        conn.close()
        return {"is_old": False}
        
    cur.execute("SELECT visit_reason FROM visits WHERE user_uuid = ? ORDER BY timestamp DESC LIMIT 1", (user_uuid,))
    last_visit = cur.fetchone()
    reason = last_visit["visit_reason"] if last_visit else "办事"
    conn.close()
    
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
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("INSERT INTO visits (user_uuid, visit_reason) VALUES (?, ?)", (req.user_uuid, req.reason))
    conn.commit()
    
    # 统计本月来访次数
    cur.execute("SELECT COUNT(*) FROM visits WHERE user_uuid = ? AND strftime('%Y-%m', timestamp) = strftime('%Y-%m', 'now')", (req.user_uuid,))
    month_count = cur.fetchone()[0]
    conn.close()
    
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
