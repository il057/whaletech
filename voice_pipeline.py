import os
from dotenv import load_dotenv
load_dotenv()

import json
import uuid
import struct
import logging
import asyncio
import sqlite3
import websockets
from fastapi import WebSocket, WebSocketDisconnect
from wechat_bot import send_visitor_notification
from database import DB_FILE

VOLC_APP_ID = os.getenv("VOLC_APP_ID")
VOLC_ACCESS_TOKEN = os.getenv("VOLC_ACCESS_TOKEN")

class VoicePipeline:
    """
    负责管理单个访客的语音实时连接会话流水线。
    与端到端大模型 (Volcano Realtime API) 直接通过 WebSocket 进行全双工交互。
    """
    def __init__(self, user_uuid: str):
        self.user_uuid = user_uuid
        self.volc_ws_url = "wss://openspeech.bytedance.com/api/v3/realtime/dialogue"
        self.session_id = str(uuid.uuid4())
        self.headers = {
            "X-Api-App-ID": VOLC_APP_ID,
            "X-Api-Access-Key": VOLC_ACCESS_TOKEN,
            "X-Api-Resource-Id": "volc.speech.dialog",
            "X-Api-App-Key": "PlgvMymc7f3tQnJ6",
            "X-Api-Connect-Id": str(uuid.uuid4())
        }
        
        self.llm_response_buffer = ""
        self.visit_recorded = False
        self.mute_tts_permanently = False

    def _build_frame(self, msg_type: int, event_id: int, serialization: int, payload: bytes = b"", is_session: bool = False) -> bytes:
        """根据 API 文档构建发往服务端的二进制协议数据帧"""
        # Byte 0: v1(1) | header_size(1) = 0x11
        # Byte 1: msg_type << 4 | flags(0b0100 代表包含EventID，即 4)
        # Byte 2: serialization << 4 | compress(0)
        # Byte 3: 0
        header = bytearray([0x11, (msg_type << 4) | 4, (serialization << 4) | 0, 0])
        
        body = struct.pack(">I", event_id) # Event ID (Big Endian)
        
        # >= 100 为会话类事件，必须附带 Session ID
        if event_id >= 100 or is_session:
            sid_bytes = self.session_id.encode('utf-8')
            body += struct.pack(">I", len(sid_bytes))
            body += sid_bytes
            
        body += struct.pack(">I", len(payload))
        body += payload
        return bytes(header) + body

    def _build_system_role(self) -> str:
        """读取本地 SQLite 获取历史数据，构造注入了上下文的 System Prompt"""
        try:
            conn = sqlite3.connect(DB_FILE)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("SELECT * FROM users WHERE uuid = ?", (self.user_uuid,))
            user = cur.fetchone()
            
            # 获取最后一次来访事由
            cur.execute("SELECT visit_reason FROM visits WHERE user_uuid = ? ORDER BY timestamp DESC LIMIT 1", (self.user_uuid,))
            last_visit = cur.fetchone()
            last_reason = last_visit["visit_reason"] if last_visit else "办事"
            
            conn.close()
            
            base_rule = (
                "你是一个干练且专业的停车场门卫，正在和前来园区的司机自然地对话。\n"
                "【核心行为要求】：\n"
                "1. 绝不讲废话，要求高效。不要说书面语，改为口语化的“去哪家公司”、“来干嘛的”、“车牌号和手机号多少”。\n"
                "2. 需收集齐四个确切信息：【车牌号】、【来访单位】、【手机号】、【干什么】。如访客未提供完整，必须追问。\n"
                "3. 【最重要规则】：当且仅当确定这四个信息已全部知晓或确认时，请【无需多言、不要做任何口语回复（不要说“好的放行”、“已通知”之类的话）】，直接闭嘴，必须且只能新起一行输出以下JSON格式以触发系统放行！\n"
                "===JSON_BEGIN==={\"name\":\"\",\"phone\":\"\",\"plate\":\"\",\"company\":\"\",\"reason\":\"\"}===JSON_END===\n"
                "4. 只要信息没收集齐，绝对不能输出JSON，必须继续开口发问！一旦收集齐，立刻只输出JSON，直接切断服务。"
            )
            
            if user and user["phone"]:
                # 老用户 (只要有保留手机号认为是有记录)
                u_name = user["name"] or ""
                u_phone = user["phone"] or ""
                u_plate = user["default_plate"] or ""
                u_co = user["default_company"] or ""
                
                greeting = f"{u_name}先生/女士" if u_name else f"尾号{u_phone[-4:]}的车主"
                
                prompt = base_rule + (
                    f"\n\n【当前访客背景】：后台查到这是老访客记录：手机[{u_phone}]、车牌[{u_plate}]、常去单位[{u_co}]，上次事由是[{last_reason}]。\n"
                    f"请务必主动打招呼核对：'{greeting}您好！还是像上次一样去{u_co}{last_reason}吗？'\n"
                    f"如果访客回答肯定的意思（如“是的”、“对”），这就直接意味着四项信息已全部集齐，你【不准再重新询问】车牌、手机等，且必须【不作任何回复】立刻利用已有信息输出JSON放行！如果访客说不是，再去追问变化的信息。"
                )
            else:
                prompt = base_rule + (
                    "\n\n【当前访客背景】：这是一位新访客，后台无记录。\n"
                    "请主动开口问好并直接询问车牌、单位、事由和电话（尽量自然地合并提问，一口气讲完）。"
                )
                
            logging.info(f"【DEBUG】本次生成的 Prompt: \n{prompt}")
            return prompt
        except Exception as e:
            logging.error(f"读取数据库构建 Prompt 失败: {e}")
            return "你是一个门卫，请自然地询问访客的车牌、手机、来访单位和事由。"

    async def connect_and_handle(self, client_ws: WebSocket):
        """主入口：建立双向 WebSocket 连接流"""
        # 与浏览器保持连接，接受字节流
        await client_ws.accept()
        
        logging.info(f"开启语音流水线，User UUID = {self.user_uuid}")

        try:
            # 连接火山引擎
            async with websockets.connect(self.volc_ws_url, extra_headers=self.headers) as volc_ws:
                # 【1】发送 StartConnection (Event 1, Text)
                await volc_ws.send(self._build_frame(msg_type=1, event_id=1, serialization=1, payload=b"{}"))
                
                # 【2】发送 StartSession (Event 100, Text)
                system_role = self._build_system_role()
                start_session_payload = {
                    "tts": {
                        "audio_config": {
                            "channel": 1,
                            "format": "pcm_s16le",
                            "sample_rate": 24000 # 前端将按照这个进行播放
                        }
                    },
                    "asr": {
                        "audio_info": {
                            "format": "pcm_s16le",
                            "sample_rate": 16000,
                            "channel": 1
                        }
                    },
                    "dialog": {
                        "bot_name": "智能门卫",
                        "system_role": system_role,
                        "extra": {
                            "model": "1.2.1.1", # O2.0版本，推理速度极快
                            "enable_conversation_truncate": True
                        }
                    }
                }
                payload_bytes = json.dumps(start_session_payload).encode('utf-8')
                await volc_ws.send(self._build_frame(msg_type=1, event_id=100, is_session=True, serialization=1, payload=payload_bytes))
                
                # 【3】主动发送打招呼事件，强迫 AI 率先开口执行 Prompt 的第一句话
                hello_payload = json.dumps({"content": "（系统通知：访客已接通语音，请你直接开口询问访客，核实登记信息，不要寒暄你好）"}).encode('utf-8')
                await volc_ws.send(self._build_frame(msg_type=1, event_id=300, is_session=True, serialization=1, payload=hello_payload))
                
                # 开始互斥收发
                recv_volc_task = asyncio.create_task(self._recv_from_volcengine(volc_ws, client_ws))
                recv_client_task = asyncio.create_task(self._recv_from_client(client_ws, volc_ws))
                
                # 任一方退出则终止
                done, pending = await asyncio.wait(
                    [recv_volc_task, recv_client_task],
                    return_when=asyncio.FIRST_COMPLETED
                )
                for task in pending:
                    task.cancel()
                    
        except WebSocketDisconnect:
            logging.info("浏览器前端主动断开")
        except Exception as e:
            logging.error(f"语音流水线运行异常: {e}")

    async def _recv_from_client(self, client_ws: WebSocket, volc_ws):
        """流式读取前端麦克风采集的 PCM 数据，立刻转发给大模型 (TaskRequest Event 200)"""
        while True:
            try:
                # 等待浏览器发送音频二进制帧
                audio_data = await client_ws.receive_bytes()
                # 构造 Audio-only 请求帧 (msg_type=2), event=200, serialization=0
                frame = self._build_frame(msg_type=2, event_id=200, serialization=0, payload=audio_data)
                await volc_ws.send(frame)
            except WebSocketDisconnect:
                break
            except Exception as e:
                logging.error(f"接收前端音频错误: {e}")
                break

    async def _recv_from_volcengine(self, volc_ws, client_ws: WebSocket):
        """流式解析模型服务端返回的数据帧，驱动 TTS 音频回传和 JSON 拦截提取"""
        while True:
            try:
                resp = await volc_ws.recv()
                if not isinstance(resp, bytes) or len(resp) < 4:
                    continue
                    
                msg_type = resp[1] >> 4
                flags = resp[1] & 0x0F
                serialization = resp[2] >> 4
                
                offset = 4
                if msg_type == 15: # Error info (4 bytes code)
                    offset += 4
                if flags & 3:      # Sequence (4 bytes)
                    offset += 4
                
                logging.debug(f"Received msg_type={msg_type}, flags={flags}, serialization={serialization}")
                
                # 如果携带Event ID (0b0100)
                if flags & 4:
                    event_id = struct.unpack(">I", resp[offset:offset+4])[0]
                    offset += 4
                    
                    if event_id >= 100:
                        # Session级别的事件必带 session_id_size 和 session_id
                        sid_size = struct.unpack(">I", resp[offset:offset+4])[0]
                        offset += 4 + sid_size
                        
                    # 针对部分可能存在的 connect_id，我们通过推算规避，直接跳到 payload
                    # 因为 payload_size 必定是 payload 前的最后 4 个字节，而 payload 必定延伸到包尾
                    # 但为了严谨，可以直接解析：
                    # 这里假设紧接着就是 payload_size (如果是 Connect 事件且无 payload 我们暂时忽略处理细节)
                    if offset + 4 <= len(resp):
                        # 判断是不是有 connect_id: 如果是 event < 100, 且剩余长度超过 4+payload_size...
                        # 我们采用最安全的倒推取 payload 避免字段不定长：
                        # 但是实际上如果按照标准顺序：
                        payload_size = struct.unpack(">I", resp[-payload_size_probe(resp):][0:4]) if False else struct.unpack(">I", resp[offset:offset+4])[0]
                        # 实际上简单的办法：
                        # Payload 占据最后的末尾，所以 payload_size 就是 resp[-payload_size - 4 : -payload_size]
                        # 不过更好的写法是接着向下读：
                        
                        # 简单的兼容处理
                        # 对于 event >= 100，目前 offset 此时恰好在 payload_size 处。
                        if event_id >= 100:
                            payload_size = struct.unpack(">I", resp[offset:offset+4])[0]
                            payload = resp[offset+4 : offset+4+payload_size]
                            
                            if event_id == 352:
                                # TTSResponse (大模型语音合成流)
                                if not self.mute_tts_permanently:
                                    await client_ws.send_bytes(payload)
                                
                            elif event_id == 550:
                                # ChatResponse (大模型文本回复流)
                                if serialization == 1:
                                    info = json.loads(payload.decode('utf-8'))
                                    text_slice = info.get("content", "")
                                    self.llm_response_buffer += text_slice
                                    
                                    # 如果模型开始输出边界符，立刻永久屏蔽后续TTS，防止客户端读出JSON代码
                                    if "===" in text_slice or "===" in self.llm_response_buffer:
                                        self.mute_tts_permanently = True
                                        
                                    if not self.mute_tts_permanently and text_slice.strip():
                                        await client_ws.send_json({"type": "ai_text", "text": text_slice})
                                    
                            elif event_id == 559:
                                # ChatEnded (大模型一句话结束)
                                await client_ws.send_json({"type": "ai_end"})
                                await self._check_and_register_visit(client_ws)
                                
                            elif event_id == 451:
                                # ASRResponse (识别到的人声文字，供后台展示)
                                if serialization == 1:
                                    info = json.loads(payload.decode('utf-8'))
                                    if info.get("results") and not info["results"][0].get("is_interim"):
                                        user_text = info['results'][0].get('text')
                                        logging.info(f"🎤 访客: {user_text}")
                                        await client_ws.send_json({"type": "user_text", "text": user_text})
                                        
                            elif msg_type == 15:
                                logging.error(f"火山引擎返回错误: {payload.decode('utf-8')}")
                                
            except Exception as e:
                logging.error(f"解析火山引擎协议帧错误: {e}")
                break

    async def _check_and_register_visit(self, client_ws: WebSocket):
        """从 LLM 分段积累的文本中匹配 JSON 以完成访问登记"""
        if self.visit_recorded:
            return

        buffer = self.llm_response_buffer
        if "===JSON_BEGIN===" in buffer and "===JSON_END===" in buffer:
            start_idx = buffer.find("===JSON_BEGIN===") + len("===JSON_BEGIN===")
            end_idx = buffer.find("===JSON_END===")
            json_str = buffer[start_idx:end_idx].strip()
            
            try:
                data = json.loads(json_str)
                name = data.get("name", "")
                phone = data.get("phone", "")
                plate = data.get("plate", "")
                company = data.get("company", "")
                reason = data.get("reason", "")
                
                # 入库并发送企业微信
                self._save_to_db(name, phone, plate, company, reason)
                
                # 统计本月第几次来访
                conn = sqlite3.connect(DB_FILE)
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM visits WHERE user_uuid = ? AND strftime('%Y-%m', timestamp) = strftime('%Y-%m', 'now')", (self.user_uuid,))
                month_count = cur.fetchone()[0]
                conn.close()
                
                notice_msg = f"(提示: 该访客本月已来访 {month_count} 次)"
                success = await send_visitor_notification(name, plate, phone, company, reason, notice_msg)
                
                if success:
                    logging.info("【DEBUG】已成功将访客信息入库并通过企业微信送出")
                else:
                    logging.error("【DEBUG】访客信息企业微信推送失败！")
                
                self.visit_recorded = True
                
                # 通知前端：业务已完成
                await client_ws.send_json({"status": "completed"})
                
            except Exception as e:
                logging.error(f"提取入库或推送时发生异常: {e}\n模型原输出: {json_str}")

    def _save_to_db(self, name, phone, plate, company, reason):
        """新用户入库 / 老用户更新，并新增 visit 记录"""
        try:
            conn = sqlite3.connect(DB_FILE)
            cur = conn.cursor()
            
            # Upsert 用户基础信息
            cur.execute("""
                INSERT INTO users (uuid, phone, name, default_plate, default_company)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(uuid) DO UPDATE SET
                    phone=excluded.phone,
                    name=excluded.name,
                    default_plate=excluded.default_plate,
                    default_company=excluded.default_company
            """, (self.user_uuid, phone, name, plate, company))
            
            # 新增单次访问记录
            cur.execute("""
                INSERT INTO visits (user_uuid, visit_reason)
                VALUES (?, ?)
            """, (self.user_uuid, f"前往{company}办理{reason}"))
            
            conn.commit()
            conn.close()
            logging.info(f"访客信息已完成静默登记: {name} ({plate})")
        except Exception as e:
            logging.error(f"保存数据入库失败: {e}")
