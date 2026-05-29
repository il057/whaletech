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
from wechat_bot import send_visitor_notification, send_human_required_notification
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
        self.turn_count = 0          # 用户发言轮次计数
        self.human_notified = False  # 是否已发送过人工协助通知
        self.hello_sent = False      # 是否已发送 SayHello（防重复）
        self.session_ready = False   # SessionStarted 是否已收到
        self.initial_greeting = ""   # AI 开场白，由 _build_system_role 计算

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
                "1. 绝不讲废话，要求高效。用简短口语追问，禁止书面表达，每次只追问最紧缺的一两项信息。\n"
                "2. 需收集齐四项确切信息：【车牌号】【来访单位】【手机号】【来访事由】。缺哪项追问哪项，不得遗漏。\n"
                "3. 【最重要规则】：四项信息全部确认后，立刻闭嘴，不作任何口语回复，只在新的一行输出以下JSON格式触发放行：\n"
                "===JSON_BEGIN==={\"name\":\"\",\"phone\":\"\",\"plate\":\"\",\"company\":\"\",\"reason\":\"\"}===JSON_END===\n"
                "   ⚠️ reason只填事由动作（送货/拜访/施工等），公司名一律填company字段。\n"
                "4. 信息未收齐前绝对不输出JSON；一旦收齐立刻只输出JSON，不说任何其他话。\n"
                "5. 【人工转接】：访客要求人工或明显不耐烦时，用自己的话自然简短告知对方稍等（一句话即可），然后立刻在新的一行单独输出===HUMAN_TRANSFER===，之后不说任何其他话，不解释自己是系统。\n"
                "6. 【严禁幻觉/严禁复述】：\n"
                "   - 绝对禁止朗读或复述访客说出的任何数字（手机号、车牌号等）。数字信息只存入JSON，不得开口说出。\n"
                "   - 绝对禁止自行脑补、猜测、补全任何信息。如果没听清或信息不完整，只说\"没听清，再说一遍\"，绝不自行填充。\n"
                "   - JSON中填写的所有内容必须是访客在本次通话中明确说出的原始内容，不得凭推测或常识编造。"
            )
            
            if user and user["phone"]:
                # 老用户 (只要有保留手机号认为是有记录)
                u_name = user["name"] or ""
                u_phone = user["phone"] or ""
                u_plate = user["default_plate"] or ""
                u_co = user["default_company"] or ""
                
                greeting = f"{u_name}先生/女士" if u_name else f"尾号{u_phone[-4:]}的车主"
                # SayHello 简短自然，用公司名而非 last_reason 字符串避免重复
                self.initial_greeting = f"{greeting}，还是跟上次一样去{u_co}吗？"
                
                prompt = base_rule + (
                    f"\n\n【当前访客背景】：后台查到这是老访客记录：手机[{u_phone}]、车牌[{u_plate}]、常去单位[{u_co}]，上次来访目的[{last_reason}]。\n"
                    f"你的开场白已经说了：'{self.initial_greeting}'，直接等访客回应，不要重复打招呼。\n"
                    f"【肯定回答处理】：如果访客回答肯定（如\"是的\"、\"对\"、\"嗯\"、\"老样子\"、\"一样\"、\"还是一样\"），"
                    f"四项信息全部确认，必须【不作任何口语回复】立刻输出以下JSON，字段直接用已知信息填充：\n"
                    f"===JSON_BEGIN==={{\"name\":\"{u_name}\",\"phone\":\"{u_phone}\",\"plate\":\"{u_plate}\",\"company\":\"{u_co}\",\"reason\":\"{last_reason}\"}}===JSON_END===\n"
                    f"【否定回答处理】：如果访客说不是/不对/去别处/其他事由：\n"
                    f"  - 车牌[{u_plate}]和手机[{u_phone}]已知，绝对不要再问！\n"
                    f"  - 只简短追问目的事由和来访单位即可。访客确认后直接输出JSON。"
                )
            else:
                self.initial_greeting = "师傅，去哪家公司？来干嘛的？车牌号和手机号也报一下。"
                prompt = base_rule + (
                    "\n\n【当前访客背景】：这是一位新访客，后台无记录。\n"
                    f"你的开场白已经说了：'{self.initial_greeting}'，直接等访客回应，不要重复打招呼。"
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
                        },
                        "vad": {
                            "silence_duration_ms": 700  # 用户停顿 700ms 后才判定一句话结束，防止 AI 过早插嘴
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
                
                # 【3】先启动接收任务，再发 hello，避免服务端响应在任务创建前到达而被丢弃
                recv_volc_task = asyncio.create_task(self._recv_from_volcengine(volc_ws, client_ws))
                recv_client_task = asyncio.create_task(self._recv_from_client(client_ws, volc_ws))
                # SayHello (event 300) 将在收到服务端 SessionStarted (event 150) 后由 recv 任务自动发出
                
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
                            
                            if event_id == 150:
                                # SessionStarted — 会话已就绪，立刻发 SayHello（无需等待音频帧）
                                self.session_ready = True
                                if not self.hello_sent:
                                    self.hello_sent = True
                                    hello_payload = json.dumps({"content": self.initial_greeting}).encode('utf-8')
                                    await volc_ws.send(self._build_frame(msg_type=1, event_id=300, is_session=True, serialization=1, payload=hello_payload))
                                    logging.info(f"✅ SessionStarted 收到，SayHello 已发送: {self.initial_greeting}")
                                    await client_ws.send_json({"type": "ai_text", "text": self.initial_greeting})
                                    await client_ws.send_json({"type": "ai_end"})
                                    
                            elif event_id == 352:
                                # TTSResponse (大模型语音合成流)
                                logging.debug(f"TTSResponse received, {len(payload)} bytes, muted={self.mute_tts_permanently}")
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
                                        if not self.mute_tts_permanently:
                                            self.mute_tts_permanently = True
                                            # 通知前端立刻清空音频播放队列，防止"等于等于等于"被读出
                                            await client_ws.send_json({"type": "stop_audio"})
                                    
                                    # 检测 AI 主动触发人工转接标记，立刻推送通知并挂断
                                    if "===HUMAN_TRANSFER===" in self.llm_response_buffer and not self.human_notified:
                                        asyncio.create_task(self._check_human_intent("转人工", client_ws))
                                        
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
                                        # 累计轮数并检测人工意图
                                        self.turn_count += 1
                                        await self._check_human_intent(user_text, client_ws)
                                        
                            elif event_id == 599:
                                # DialogCommonError — 火山对话通用错误
                                if serialization == 1:
                                    err_info = json.loads(payload.decode('utf-8'))
                                    logging.error(f"🔴 火山对话错误 event=599: {err_info}")
                                    
                            elif msg_type == 15:
                                # 协议级错误帧
                                try:
                                    logging.error(f"🔴 火山协议错误 msg_type=15, event={event_id}: {payload.decode('utf-8')}")
                                except Exception:
                                    logging.error(f"🔴 火山协议错误 msg_type=15, event={event_id}, raw={payload.hex()[:40]}")
                            else:
                                logging.info(f"ℹ️ 火山未处理事件 event_id={event_id}, msg_type={msg_type}, payload_size={len(payload)}")
                                
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
                
                notice_msg = f"提示: 该访客本月已来访 {month_count} 次"
                success = await send_visitor_notification(name, plate, phone, company, reason, notice_msg)
                
                if success:
                    logging.info("【DEBUG】已成功将访客信息入库并通过微信送出")
                else:
                    logging.error("【DEBUG】访客信息微信推送失败！")
                
                self.visit_recorded = True
                
                # 通知前端：业务已完成
                await client_ws.send_json({"status": "completed"})
                
            except Exception as e:
                logging.error(f"提取入库或推送时发生异常: {e}\n模型原输出: {json_str}")

    def _extract_partial_info(self) -> dict:
        """尝试从 LLM 响应缓冲区中提取已收集的访客信息（完整或部分 JSON）"""
        buffer = self.llm_response_buffer
        if "===JSON_BEGIN===" in buffer and "===JSON_END===" in buffer:
            start_idx = buffer.find("===JSON_BEGIN===") + len("===JSON_BEGIN===")
            end_idx = buffer.find("===JSON_END===")
            try:
                return json.loads(buffer[start_idx:end_idx].strip())
            except Exception:
                pass
        return {}

    HUMAN_INTENT_KEYWORDS = [
        "叫人", "转人工", "找人工", "要人工", "人工服务", "要真人", "找真人",
        "让人来", "叫保安", "找保安", "算了", "不用了", "不想说了", "帮我叫",
        "不知道", "不清楚", "我不会", "叫个人来", "有没有人"
    ]
    MAX_TURNS_BEFORE_HUMAN = 8  # 超过此轮数仍未登记，触发人工

    async def _check_human_intent(self, user_text: str, client_ws: WebSocket = None):
        """检测用户语音中是否含有人工协助意图，或对话轮数过多，触发后推送人工协助通知并挂断"""
        if self.human_notified or self.visit_recorded:
            return

        has_keyword = any(kw in user_text for kw in self.HUMAN_INTENT_KEYWORDS)
        has_too_many_turns = self.turn_count >= self.MAX_TURNS_BEFORE_HUMAN

        if has_keyword or has_too_many_turns:
            self.human_notified = True
            partial = self._extract_partial_info()
            
            # 对于回访用户，从缓冲区提取的信息可能为空（因为AI直接用系统上下文中的记录）
            # 此时回退查数据库补充已知信息
            if not partial.get("phone") or not partial.get("plate"):
                try:
                    conn = sqlite3.connect(DB_FILE)
                    conn.row_factory = sqlite3.Row
                    cur = conn.cursor()
                    cur.execute("SELECT * FROM users WHERE uuid = ?", (self.user_uuid,))
                    user_row = cur.fetchone()
                    conn.close()
                    if user_row:
                        if not partial.get("name") and user_row["name"]:
                            partial["name"] = user_row["name"]
                        if not partial.get("phone") and user_row["phone"]:
                            partial["phone"] = user_row["phone"]
                        if not partial.get("plate") and user_row["default_plate"]:
                            partial["plate"] = user_row["default_plate"]
                        if not partial.get("company") and user_row["default_company"]:
                            partial["company"] = user_row["default_company"]
                except Exception as db_err:
                    logging.error(f"人工通知时读取DB失败: {db_err}")
            
            if has_keyword:
                trigger_reason = f"用户主动请求人工（原话：{user_text}）"
            else:
                trigger_reason = f"对话已进行 {self.turn_count} 轮，信息仍未采集完整"
            logging.info(f"[人工协助] 触发通知，原因: {trigger_reason}")
            await send_human_required_notification(
                partial.get("name", ""),
                partial.get("plate", ""),
                partial.get("phone", ""),
                partial.get("company", ""),
                partial.get("reason", ""),
                trigger_reason
            )

            # 将未完成的访客信息保存至 pending_human_cases 供保安事后补录
            try:
                conn = sqlite3.connect(DB_FILE)
                cur = conn.cursor()
                cur.execute("""
                    INSERT INTO pending_human_cases
                        (user_uuid, partial_name, partial_phone, partial_plate, partial_company, partial_reason, trigger_reason)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    self.user_uuid,
                    partial.get("name", ""),
                    partial.get("phone", ""),
                    partial.get("plate", ""),
                    partial.get("company", ""),
                    partial.get("reason", ""),
                    trigger_reason
                ))
                conn.commit()
                conn.close()
                logging.info("[人工协助] 已将待处理记录写入 pending_human_cases")
            except Exception as db_err:
                logging.error(f"保存 pending_human_cases 失败: {db_err}")

            # 人工接管后挂断电话：通知前端停止并关闭连接
            if client_ws:
                try:
                    await client_ws.send_json({"type": "hang_up"})
                    await client_ws.close()
                except Exception:
                    pass

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
            """, (self.user_uuid, reason))
            
            conn.commit()
            conn.close()
            logging.info(f"访客信息已完成静默登记: {name} ({plate})")
        except Exception as e:
            logging.error(f"保存数据入库失败: {e}")
