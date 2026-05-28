import os
import json
import time
import base64
import random
import asyncio
import logging
from typing import Optional, Dict, Any
import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

DEFAULT_BASE_URL = "https://ilinkai.weixin.qq.com"
BOT_TYPE = "3"
CHANNEL_VERSION = "1.0.2"
TOKEN_FILE = ".wechat_token.json"

def random_wechat_uin() -> str:
    """
    UIN 随机头生成：生成 4 字节的随机 uint32（大端解码相当于直接取随机数的整数值），
    转为以 10 进制字符串形式存在的 Base64 编码。
    参考 cc-weixin 中的：crypto.randomBytes(4).readUInt32BE(0) 
    """
    uint32_val = random.getrandbits(32)
    return base64.b64encode(str(uint32_val).encode("utf-8")).decode("utf-8")

def get_auth_headers(token: str = None, body_bytes: bytes = None) -> dict:
    headers = {
        "Content-Type": "application/json",
        "AuthorizationType": "ilink_bot_token",
        "X-WECHAT-UIN": random_wechat_uin(),
    }
    if body_bytes is not None:
        headers["Content-Length"] = str(len(body_bytes))
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers

def load_session() -> Optional[Dict[str, Any]]:
    if not os.path.exists(TOKEN_FILE):
        return None
    try:
        with open(TOKEN_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"加载 Token 文件失败: {e}")
        return None

def save_session(session_data: Dict[str, Any]):
    with open(TOKEN_FILE, "w", encoding="utf-8") as f:
        json.dump(session_data, f, ensure_ascii=False, indent=2)

async def login_with_qrcode() -> Dict[str, Any]:
    """
    扫码登录主逻辑，循环检测直到成功，保存 token 并返回。
    """
    logging.info("🔐 开始微信扫码登录...")
    
    async with httpx.AsyncClient(timeout=15.0) as client:
        # 1. 获取二维码
        url = f"{DEFAULT_BASE_URL}/ilink/bot/get_bot_qrcode?bot_type={BOT_TYPE}"
        try:
            resp = await client.get(url)
            resp.raise_for_status()
            qr_data = resp.json()
        except Exception as e:
            logging.error(f"获取登录二维码失败: {e}")
            raise
            
        current_qrcode = qr_data.get("qrcode")
        qrcode_img_url = qr_data.get("qrcode_img_content")
        
        logging.info("📱 请用微信扫描以下链接或二维码：")
        print("="*60)
        print(f"请在浏览器中打开链接并在微信中扫码:\n{qrcode_img_url}")
        print("="*60)
        
        try:
            import qrcode
            qr = qrcode.QRCode()
            qr.add_data(qrcode_img_url)
            qr.print_ascii(invert=True)
        except ImportError:
            logging.info("(安装 qrcode 库可在终端直接显示二维码)")
            
        deadline = time.time() + 5 * 60  # 5 分钟超时
        refresh_count = 0
        
        # 2. 轮询二维码状态
        while time.time() < deadline:
            poll_url = f"{DEFAULT_BASE_URL}/ilink/bot/get_qrcode_status?qrcode={current_qrcode}"
            try:
                # 轮询需要超时处理
                poll_resp = await client.get(poll_url, timeout=35.0)
                poll_resp.raise_for_status()
                status_data = poll_resp.json()
            except httpx.ReadTimeout:
                # 客户端长连接超时，正常继续轮询
                continue
            except Exception as e:
                logging.error(f"轮询状态失败: {e}")
                await asyncio.sleep(2)
                continue
                
            status = status_data.get("status")
            
            if status == "wait":
                # 等待中
                await asyncio.sleep(1)
            elif status == "scaned":
                logging.info("👀 已扫码，请在微信端确认...")
                await asyncio.sleep(1)
            elif status == "expired":
                refresh_count += 1
                if refresh_count > 3:
                    raise Exception("二维码多次过期，请重新运行。")
                logging.info(f"⏳ 二维码过期，刷新中 ({refresh_count}/3)...")
                
                resp = await client.get(f"{DEFAULT_BASE_URL}/ilink/bot/get_bot_qrcode?bot_type={BOT_TYPE}")
                qr_data = resp.json()
                current_qrcode = qr_data.get("qrcode")
                qrcode_img_url = qr_data.get("qrcode_img_content")
                print("\n[二维码已刷新] 请扫描新链接:")
                print(qrcode_img_url)
                try:
                    qr = qrcode.QRCode()
                    qr.add_data(qrcode_img_url)
                    qr.print_ascii(invert=True)
                except:
                    pass
            elif status == "confirmed":
                logging.info("✅ 登录成功！")
                session = {
                    "token": status_data.get("bot_token"),
                    "baseUrl": status_data.get("baseurl", DEFAULT_BASE_URL),
                    "accountId": status_data.get("ilink_bot_id"),
                    "userId": status_data.get("ilink_user_id"), # 用作管理员推送目标
                    "savedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ")
                }
                save_session(session)
                logging.info(f"Bot ID: {session['accountId']}")
                return session
            else:
                logging.warning(f"未知的登录状态: {status}")
                await asyncio.sleep(1)
                
        raise Exception("扫码登录超时")

async def ensure_session() -> Dict[str, Any]:
    session = load_session()
    if session and session.get("token"):
        return session
    return await login_with_qrcode()

async def send_visitor_notification(name: str, plate: str, phone: str, company: str, reason: str, notice: str = "") -> bool:
    session = load_session()
    if not session or not session.get("token"):
        logging.error("微信 Token 未就绪，无法发送访客通知，请先启动服务扫码登录。")
        return False

    base_url = session.get("baseUrl", DEFAULT_BASE_URL)
    token = session.get("token")
    to_user_id = session.get("userId")  # 默认推送给扫码绑定的主用户(保安)
    
    if not to_user_id:
        logging.error("找不到推送目标的 user_id。")
        return False
        
    text_content = (
        f"🚨 访客提醒\n\n"
        f"姓名: {name}\n"
        f"车牌: {plate}\n"
        f"电话: {phone}\n"
        f"单位: {company}\n"
        f"事由: {reason}\n"
        + (f"\n补充说明: {notice}" if notice else "")
    )
    
    await send_text_message(base_url, token, to_user_id, text_content)
    return True

async def send_text_message(base_url: str, token: str, to_user_id: str, text: str, context_token: str = None):
    endpoint = f"{base_url.rstrip('/')}/ilink/bot/sendmessage"
    
    # 构建WeixinMessage 结构
    msg_payload = {        "from_user_id": "",        "to_user_id": to_user_id,
        "client_id": f"msg_{int(time.time()*1000)}_{random.randint(1000, 9999)}",
        "message_type": 2, # BOT
        "message_state": 2, # FINISH
        "item_list": [
            {
                "type": 1, # TEXT
                "text_item": {
                    "text": text
                }
            }
        ]
    }
    if context_token:
        msg_payload["context_token"] = context_token
        
    body = {
        "msg": msg_payload,
        "base_info": { "channel_version": CHANNEL_VERSION }
    }
    
    body_bytes = json.dumps(body).encode("utf-8")
    headers = get_auth_headers(token, body_bytes)
    
    # 令牌过期可能导致响应 401，这里简单进行推送，实际工程中可考虑增加 token 重登判断
    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            resp = await client.post(endpoint, content=body_bytes, headers=headers)
            resp_text = resp.text
            if not resp.is_success:
                logging.error(f"消息发送失败: HTTP {resp.status_code} - {resp_text}")
                # 【静默重连逻辑】: 当 Token 过期失效时 (通常表现为 401 错误或 body 包含 Token Invalid)
                # 我们清理本地缓存，下一次操作 (如长轮询或推送) 时将自动触发确保会话逻辑重新出现二维码，实现准静默恢复。
                if "Token Invalid" in resp_text or resp.status_code == 401:
                    logging.warning("Token 可能已失效，已清除本地缓存，将在下一次检查时重新要求扫码。")
                    if os.path.exists(TOKEN_FILE):
                        os.remove(TOKEN_FILE)
            else:
                logging.info(f"消息推送成功 -> {text[:15]}...")
        except Exception as e:
            logging.error(f"调用发送消息接口异常: {e}")

async def get_updates(base_url: str, token: str, sync_buf: str = "") -> Dict[str, Any]:
    """
    长轮询接口，获取服务端推送的新消息。
    当没有消息时服务端将请求 Hold 住直到超时（默认约 30 秒）。
    """
    endpoint = f"{base_url.rstrip('/')}/ilink/bot/getupdates"
    body = {
        "get_updates_buf": sync_buf,
        "base_info": {"channel_version": CHANNEL_VERSION}
    }
    
    body_bytes = json.dumps(body).encode("utf-8")
    headers = get_auth_headers(token, body_bytes)
    
    # 鉴于服务端会 Hold 请求，这里的 timeout 应该略大于 服务端的默认超时 (30~35s)
    async with httpx.AsyncClient(timeout=40.0) as client:
        try:
            resp = await client.post(endpoint, content=body_bytes, headers=headers)
            resp_text = resp.text
            if not resp.is_success:
                if "Token Invalid" in resp_text or resp.status_code == 401:
                    logging.warning("get_updates 发现 Token 已失效，已清除本地缓存。")
                    if os.path.exists(TOKEN_FILE):
                        os.remove(TOKEN_FILE)
                raise Exception(f"get_updates 失败: HTTP {resp.status_code} - {resp_text}")
            
            return resp.json()
        except httpx.ReadTimeout:
            # 长轮询正常超时，返回空信息
            return {"ret": 0, "msgs": [], "get_updates_buf": sync_buf}
