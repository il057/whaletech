import os
from dotenv import load_dotenv
load_dotenv()

import json
import uuid
import struct
import logging
import asyncio
import sqlite3          # 仅用于 asyncio.to_thread 包装的同步辅助函数
import websockets
from fastapi import WebSocket, WebSocketDisconnect
from wechat_bot import send_visitor_notification, send_human_required_notification
from database import DB_FILE
from validator import validate_visitor_data

VOLC_APP_ID = os.getenv("VOLC_APP_ID")
VOLC_ACCESS_TOKEN = os.getenv("VOLC_ACCESS_TOKEN")

# ─────────────────────────────────────────────────────────────────────────────
# VoicePipeline  v3
# 核心改动：
#  1. _handle_interrupt 不再清空含 JSON 的 buffer（避免校验数据丢失）
#  2. connect_and_handle 使用 while 循环：校验失败时静默重启 Volcengine session，
#     新 session 通过 SayHello 播放纠错语音，只追问单个字段
#  3. 550 文本流一旦检测到 === 立即停发 ai_text，并设 mute_tts_permanently
#  4. 352 TTS 帧：buffer 末尾出现 "=" 或 "==" 时提前发 stop_audio 拦截"等于"
# ─────────────────────────────────────────────────────────────────────────────
class VoicePipeline:

    def __init__(self, user_uuid: str):
        self.user_uuid = user_uuid
        self.volc_ws_url = "wss://openspeech.bytedance.com/api/v3/realtime/dialogue"

        # 会话层（每次 Volcengine session 均重置）
        self.session_id           = str(uuid.uuid4())
        self.llm_response_buffer  = ""
        self.mute_tts_permanently = False
        self._interrupt_flag      = False
        self.session_ready        = False
        self.hello_sent           = False
        self.initial_greeting     = ""
        self._current_greeting    = ""

        # 业务层（整个通话生命周期有效）
        self.visit_recorded = False
        self.human_notified = False
        self.turn_count     = 0

        # 纠错状态（校验失败时设置，供下一 session 使用）
        self._correction_context: dict | None  = None
        self._session_exit_reason: str | None  = None

    # ─────────────────────────────────────────────────────────────────────────
    # 协议帧构造
    # ─────────────────────────────────────────────────────────────────────────
    def _build_frame(self, msg_type: int, event_id: int, serialization: int,
                     payload: bytes = b"", is_session: bool = False) -> bytes:
        header = bytearray([0x11, (msg_type << 4) | 4, (serialization << 4) | 0, 0])
        body   = struct.pack(">I", event_id)
        if event_id >= 100 or is_session:
            sid = self.session_id.encode('utf-8')
            body += struct.pack(">I", len(sid)) + sid
        body += struct.pack(">I", len(payload)) + payload
        return bytes(header) + body

    def _make_headers(self) -> dict:
        """每个 Volcengine session 使用独立的 Connect-Id"""
        return {
            "X-Api-App-ID":      VOLC_APP_ID,
            "X-Api-Access-Key":  VOLC_ACCESS_TOKEN,
            "X-Api-Resource-Id": "volc.speech.dialog",
            "X-Api-App-Key":     "PlgvMymc7f3tQnJ6",
            "X-Api-Connect-Id":  str(uuid.uuid4()),
        }

    # ─────────────────────────────────────────────────────────────────────────
    # System Prompt 构建
    # ─────────────────────────────────────────────────────────────────────────
    def _build_system_role(self) -> str:
        """首次 session：读历史数据，构造完整 System Prompt。

        注意：本方法是同步的，调用方 connect_and_handle 应通过
        asyncio.to_thread(_build_system_role_sync, self.user_uuid) 调用，
        避免阻塞事件循环。此处保留同步签名供 to_thread 内部使用。
        """
        try:
            conn = sqlite3.connect(DB_FILE)
            conn.row_factory = sqlite3.Row
            cur  = conn.cursor()
            cur.execute("SELECT * FROM users WHERE uuid = ?", (self.user_uuid,))
            user = cur.fetchone()
            cur.execute(
                "SELECT visit_reason FROM visits WHERE user_uuid = ? ORDER BY timestamp DESC LIMIT 1",
                (self.user_uuid,)
            )
            last_visit  = cur.fetchone()
            last_reason = last_visit["visit_reason"] if last_visit else "办事"
            conn.close()

            base_rule = (
                "你是一个干练且专业的停车场门卫，正在和前来园区的司机自然地对话。\n"
                "【核心行为要求】：\n"
                "1. 绝不讲废话，高效简洁。用简短口语追问，每次只追问最紧缺的一两项信息。\n"
                "2. 需收集齐四项确切信息：【车牌号】【来访单位】【手机号】【来访事由】。缺哪项追问哪项。\n"
                "3. 【JSON输出 — 最高优先级规则】：四项信息全部确认后，\n"
                "   你的整条回复只能是这一行JSON，不能有任何其他文字——不说好的、不说确认、什么都不说：\n"
                "   ===JSON_BEGIN==={\"name\":\"\",\"phone\":\"\",\"plate\":\"\",\"company\":\"\",\"reason\":\"\"}===JSON_END===\n"
                "   ⚠️ JSON之前一个字都没有，JSON之后一个字都没有，你的回复100%就是这一行。\n"
                "   ⚠️ reason只填事由动作（送货/拜访/施工等），公司名填company字段。\n"
                "4. 信息未收齐前绝对不输出JSON。\n"
                "5. 【人工转接】：访客要求人工时，一句话告知稍等，然后在新行输出===HUMAN_TRANSFER===，之后不说任何话。\n"
                "6. 【严禁幻觉/复述】：禁止朗读任何数字。禁止猜测信息，没听清只说'再说一遍'。\n"
                "   JSON中填写的所有内容必须是访客本次通话中明确说出的原始内容。"
            )

            if user and user["phone"]:
                u_name  = user["name"] or ""
                u_phone = user["phone"] or ""
                u_plate = user["default_plate"] or ""
                u_co    = user["default_company"] or ""
                greeting = f"{u_name}先生/女士" if u_name else f"尾号{u_phone[-4:]}的车主"
                self.initial_greeting = f"{greeting}，还是跟上次一样去{u_co}吗？"
                prompt = base_rule + (
                    f"\n\n【当前访客背景】：老访客，手机[{u_phone}]、车牌[{u_plate}]、常去单位[{u_co}]，上次目的[{last_reason}]。\n"
                    f"你的开场白已说了：'{self.initial_greeting}'，直接等访客回应，不要重复打招呼。\n"
                    f"【肯定回答】：访客回答肯定（是/对/嗯/老样子），四项信息全部确认，立刻只输出JSON：\n"
                    f"===JSON_BEGIN==={{\"name\":\"{u_name}\",\"phone\":\"{u_phone}\",\"plate\":\"{u_plate}\",\"company\":\"{u_co}\",\"reason\":\"{last_reason}\"}}===JSON_END===\n"
                    f"【否定回答】：车牌[{u_plate}]和手机[{u_phone}]已知，绝对不问！只追问事由和单位，确认后直接输出JSON。"
                )
            else:
                self.initial_greeting = "师傅，去哪家公司？来干嘛的？车牌号和手机号也报一下。"
                prompt = base_rule + (
                    "\n\n【当前访客背景】：新访客，无历史记录。\n"
                    f"你的开场白已说了：'{self.initial_greeting}'，直接等访客回应。"
                )

            logging.info(f"[SystemPrompt] 首次 session，长度={len(prompt)}")
            return prompt
        except Exception as e:
            logging.error(f"构建 System Prompt 失败: {e}")
            return "你是门卫，请自然地询问访客车牌、手机、来访单位和事由，收集齐后只输出JSON。"

    def _build_correction_role(self) -> str:
        """纠错 session：System Prompt 只聚焦追问那一个有问题的字段"""
        ctx     = self._correction_context
        name    = ctx.get("name", "")
        phone   = ctx.get("phone", "")
        plate   = ctx.get("plate", "")
        company = ctx.get("company", "")
        reason  = ctx.get("reason", "")
        field   = ctx["invalid_field"]

        if field == "phone":
            known     = f"车牌[{plate}]、来访单位[{company}]、来访事由[{reason}]"
            if name: known = f"姓名[{name}]、" + known
            err_desc  = f"刚才收到的手机号[{phone}]格式不正确（需要1开头的11位大陆手机号）"
            ask_label = "手机号"
            json_tpl  = (
                f'{{"name":"{name}","phone":"<访客重新报的手机号>",'
                f'"plate":"{plate}","company":"{company}","reason":"{reason}"}}'
            )
        else:
            known     = f"手机[{phone}]、来访单位[{company}]、来访事由[{reason}]"
            if name: known = f"姓名[{name}]、" + known
            err_desc  = f"刚才收到的车牌[{plate}]格式不正确（需标准大陆车牌，如沪A12345）"
            ask_label = "车牌号"
            json_tpl  = (
                f'{{"name":"{name}","phone":"{phone}",'
                f'"plate":"<访客重新报的车牌>","company":"{company}","reason":"{reason}"}}'
            )

        return (
            "你是停车场门卫。\n"
            f"【已确认信息】：{known}。\n"
            f"【问题】：{err_desc}。\n"
            f"【你的任务】：直接简短告知访客{ask_label}格式有误，请他重说一遍。不要问其他任何信息。\n"
            f"收到正确的{ask_label}后，你的整条回复只有一行JSON，前后绝对没有任何文字：\n"
            f"===JSON_BEGIN==={json_tpl}===JSON_END===\n"
            f"将 <访客重新报的{ask_label}> 替换成访客说的正确内容。\n"
            f"⚠️ JSON之前和之后一个字都不能有。"
        )

    def _build_correction_greeting(self) -> str:
        field = self._correction_context["invalid_field"]
        if field == "phone":
            return "刚才您报的手机号格式不对，麻烦重新说一下完整的11位手机号。"
        return "刚才您报的车牌格式有点问题，麻烦重新说一下完整车牌。"

    # ─────────────────────────────────────────────────────────────────────────
    # 打断处理
    # ─────────────────────────────────────────────────────────────────────────
    async def _handle_interrupt(self, client_ws: WebSocket):
        """
        处理前端打断信令。
        关键修复：若 buffer 已含 JSON 数据，保留 buffer（只重置 mute 标志），
        避免 559 事件到来时找不到 JSON 导致校验从不执行。
        """
        logging.info("⚡ 收到打断信令")
        self._interrupt_flag = True
        try:
            await client_ws.send_json({"type": "stop_audio"})
        except Exception:
            pass

        if not self.visit_recorded:
            if "===JSON_BEGIN===" in self.llm_response_buffer:
                # buffer 含 JSON → 保留数据，只重置 mute 标志
                self.mute_tts_permanently = False
                logging.info("⚡ 打断时 buffer 含 JSON，保留数据等待 559 校验")
            else:
                # 普通对话轮次 → 正常清空
                self.llm_response_buffer  = ""
                self.mute_tts_permanently = False

    # ─────────────────────────────────────────────────────────────────────────
    # 主入口（while 循环支持 session 重启）
    # ─────────────────────────────────────────────────────────────────────────
    async def connect_and_handle(self, client_ws: WebSocket):
        await client_ws.accept()
        logging.info(f"[Pipeline] 启动，user_uuid={self.user_uuid}")

        while True:
            # ── 每次 session 重置会话层状态 ──────────────────────────────
            self.session_id           = str(uuid.uuid4())
            self.llm_response_buffer  = ""
            self.mute_tts_permanently = False
            self._interrupt_flag      = False
            self.session_ready        = False
            self.hello_sent           = False
            self._session_exit_reason = None

            # ── 确定本 session 使用的 System Prompt 和开场白 ─────────────
            if self._correction_context is not None:
                system_role            = self._build_correction_role()
                self._current_greeting = self._build_correction_greeting()
                logging.info(
                    f"[Pipeline] 纠错 session，追问字段={self._correction_context['invalid_field']}"
                )
            else:
                # _build_system_role 内含同步 sqlite3.connect，用 to_thread
                # 防止阻塞事件循环，让其他 WebSocket 连接正常调度
                system_role            = await asyncio.to_thread(self._build_system_role)
                self._current_greeting = self.initial_greeting

            # ── 建立 Volcengine 连接 ─────────────────────────────────────
            try:
                async with websockets.connect(
                    self.volc_ws_url,
                    extra_headers=self._make_headers()
                ) as volc_ws:

                    # StartConnection (event 1)
                    await volc_ws.send(
                        self._build_frame(msg_type=1, event_id=1,
                                          serialization=1, payload=b"{}")
                    )
                    # StartSession (event 100)
                    sess_payload = json.dumps({
                        "tts": {
                            "audio_config": {
                                "channel": 1, "format": "pcm_s16le", "sample_rate": 24000
                            }
                        },
                        "asr": {
                            "audio_info": {
                                "format": "pcm_s16le", "sample_rate": 16000, "channel": 1
                            },
                            "vad": {"silence_duration_ms": 700}
                        },
                        "dialog": {
                            "bot_name": "智能门卫",
                            "system_role": system_role,
                            "extra": {
                                "model": "1.2.1.1",
                                "enable_conversation_truncate": True
                            }
                        }
                    }).encode('utf-8')
                    await volc_ws.send(
                        self._build_frame(msg_type=1, event_id=100, is_session=True,
                                          serialization=1, payload=sess_payload)
                    )

                    recv_volc   = asyncio.create_task(
                        self._recv_from_volcengine(volc_ws, client_ws)
                    )
                    recv_client = asyncio.create_task(
                        self._recv_from_client(client_ws, volc_ws)
                    )

                    done, pending = await asyncio.wait(
                        [recv_volc, recv_client],
                        return_when=asyncio.FIRST_COMPLETED
                    )
                    for t in pending:
                        t.cancel()
                        try:
                            await t
                        except asyncio.CancelledError:
                            pass

            except WebSocketDisconnect:
                logging.info("[Pipeline] 前端主动断开")
                break
            except Exception as e:
                logging.error(f"[Pipeline] session 异常: {e}")
                break

            reason = self._session_exit_reason
            logging.info(f"[Pipeline] session 结束，reason={reason}")

            if reason == "validation_failed":
                # 解锁前端音频，短暂等待后启动纠错 session
                try:
                    await client_ws.send_json({"type": "resume_audio"})
                except Exception:
                    pass
                await asyncio.sleep(0.3)
                continue   # 以 _correction_context 重启

            break   # completed / hangup / error

        logging.info("[Pipeline] 通话流水线结束")

    # ─────────────────────────────────────────────────────────────────────────
    # 接收前端消息
    # ─────────────────────────────────────────────────────────────────────────
    async def _recv_from_client(self, client_ws: WebSocket, volc_ws):
        while True:
            try:
                msg = await client_ws.receive()
                if msg.get("type") == "websocket.disconnect":
                    break
                audio_data = msg.get("bytes")
                text_data  = msg.get("text")
                if audio_data:
                    await volc_ws.send(
                        self._build_frame(msg_type=2, event_id=200,
                                          serialization=0, payload=audio_data)
                    )
                elif text_data:
                    try:
                        sig = json.loads(text_data)
                        if sig.get("action") == "interrupt":
                            await self._handle_interrupt(client_ws)
                    except Exception as pe:
                        logging.warning(f"解析前端信令失败: {pe}")
            except WebSocketDisconnect:
                break
            except Exception as e:
                logging.error(f"接收前端消息错误: {e}")
                break

    # ─────────────────────────────────────────────────────────────────────────
    # 接收 Volcengine 消息
    # ─────────────────────────────────────────────────────────────────────────
    async def _recv_from_volcengine(self, volc_ws, client_ws: WebSocket):
        while True:
            try:
                resp = await volc_ws.recv()
                if not isinstance(resp, bytes) or len(resp) < 4:
                    continue

                msg_type      = resp[1] >> 4
                flags         = resp[1] & 0x0F
                serialization = resp[2] >> 4

                offset = 4
                if msg_type == 15: offset += 4
                if flags & 3:      offset += 4

                if not (flags & 4):
                    continue   # 无 Event ID

                event_id = struct.unpack(">I", resp[offset:offset+4])[0]
                offset  += 4

                if event_id >= 100:
                    sid_size = struct.unpack(">I", resp[offset:offset+4])[0]
                    offset  += 4 + sid_size

                if offset + 4 > len(resp):
                    continue

                payload_size = struct.unpack(">I", resp[offset:offset+4])[0]
                payload      = resp[offset+4 : offset+4+payload_size]

                # ── 150: SessionStarted ────────────────────────────────────
                if event_id == 150:
                    self.session_ready = True
                    if not self.hello_sent:
                        self.hello_sent = True
                        greeting = self._current_greeting
                        hello_pl = json.dumps({"content": greeting}).encode('utf-8')
                        await volc_ws.send(
                            self._build_frame(msg_type=1, event_id=300,
                                              is_session=True, serialization=1,
                                              payload=hello_pl)
                        )
                        logging.info(f"✅ SayHello 已发送: {greeting}")
                        try:
                            await client_ws.send_json({"type": "ai_text", "text": greeting})
                            await client_ws.send_json({"type": "ai_end"})
                        except Exception:
                            pass

                # ── 352: TTS 音频 ──────────────────────────────────────────
                elif event_id == 352:
                    # 提前拦截：buffer 末尾出现 "=" 表明 "===" 正在积累
                    buf_tail = self.llm_response_buffer[-3:]
                    if (not self.mute_tts_permanently and
                            (buf_tail.endswith("==") or
                             buf_tail.endswith("=") or
                             "===" in self.llm_response_buffer)):
                        self.mute_tts_permanently = True
                        logging.info("[TTS] 预判 === 即将出现，提前屏蔽 TTS")
                        try:
                            await client_ws.send_json({"type": "stop_audio"})
                        except Exception:
                            pass

                    if not self.mute_tts_permanently and not self._interrupt_flag:
                        try:
                            await client_ws.send_bytes(payload)
                        except Exception:
                            pass

                # ── 550: 文本流 ────────────────────────────────────────────
                elif event_id == 550:
                    if serialization != 1:
                        continue
                    info       = json.loads(payload.decode('utf-8'))
                    text_slice = info.get("content", "")
                    self.llm_response_buffer += text_slice

                    # 一旦出现 === 立即屏蔽后续 TTS
                    if "===" in self.llm_response_buffer and not self.mute_tts_permanently:
                        self.mute_tts_permanently = True
                        logging.info("[TTS] 检测到 ===，永久屏蔽 TTS")
                        try:
                            await client_ws.send_json({"type": "stop_audio"})
                        except Exception:
                            pass

                    # 人工转接检测
                    if ("===HUMAN_TRANSFER===" in self.llm_response_buffer
                            and not self.human_notified):
                        asyncio.create_task(self._check_human_intent("转人工", client_ws))

                    # 只在非 JSON 轮次向前端推文字（已屏蔽则跳过）
                    if not self.mute_tts_permanently and text_slice.strip():
                        try:
                            await client_ws.send_json({"type": "ai_text", "text": text_slice})
                        except Exception:
                            pass

                # ── 559: ChatEnded ─────────────────────────────────────────
                elif event_id == 559:
                    is_json_turn = "===JSON_BEGIN===" in self.llm_response_buffer

                    if not is_json_turn:
                        # 普通对话轮次
                        self._interrupt_flag = False
                        try:
                            await client_ws.send_json({"type": "ai_end"})
                        except Exception:
                            pass
                    else:
                        # JSON 轮次：进入校验流程
                        await self._check_and_register_visit(client_ws)
                        if self._session_exit_reason == "validation_failed":
                            break   # 退出 recv 循环，触发 session 重启

                # ── 451: ASR 识别结果 ──────────────────────────────────────
                elif event_id == 451:
                    self._interrupt_flag = False
                    if serialization != 1:
                        continue
                    info    = json.loads(payload.decode('utf-8'))
                    results = info.get("results", [])
                    if results and not results[0].get("is_interim"):
                        user_text = results[0].get("text", "")
                        logging.info(f"🎤 访客: {user_text}")
                        try:
                            await client_ws.send_json({"type": "user_text", "text": user_text})
                        except Exception:
                            pass
                        self.turn_count += 1
                        await self._check_human_intent(user_text, client_ws)

                # ── 599: 对话错误 ──────────────────────────────────────────
                elif event_id == 599:
                    if serialization == 1:
                        logging.error(
                            f"🔴 火山对话错误 599: {json.loads(payload.decode('utf-8'))}"
                        )

                # ── msg_type 15: 协议错误 ─────────────────────────────────
                elif msg_type == 15:
                    try:
                        logging.error(f"🔴 协议错误 event={event_id}: {payload.decode('utf-8')}")
                    except Exception:
                        logging.error(f"🔴 协议错误 event={event_id} raw={payload.hex()[:40]}")

                else:
                    logging.debug(f"ℹ️ 未处理事件 event_id={event_id}")

            except Exception as e:
                logging.error(f"解析 Volcengine 帧错误: {e}")
                break

    # ─────────────────────────────────────────────────────────────────────────
    # 数据校验 + 入库
    # ─────────────────────────────────────────────────────────────────────────
    async def _check_and_register_visit(self, client_ws: WebSocket):
        """
        从 llm_response_buffer 提取 JSON，校验 phone/plate。
        通过则入库 + 推送企业微信；失败则保存纠错上下文并信号 session 重启。
        """
        if self.visit_recorded:
            return

        buffer = self.llm_response_buffer
        if "===JSON_BEGIN===" not in buffer or "===JSON_END===" not in buffer:
            logging.warning("[数据校验] buffer 中未找到完整 JSON，跳过")
            return

        start   = buffer.find("===JSON_BEGIN===") + len("===JSON_BEGIN===")
        end     = buffer.find("===JSON_END===")
        json_str = buffer[start:end].strip()

        try:
            data    = json.loads(json_str)
            name    = data.get("name", "")
            phone   = data.get("phone", "")
            plate   = data.get("plate", "")
            company = data.get("company", "")
            reason  = data.get("reason", "")
        except Exception as e:
            logging.error(f"[数据校验] JSON 解析失败: {e}，原文: {json_str}")
            return

        # ── 数据清洗拦截 ──────────────────────────────────────────────────
        is_valid, failed_field, _ = validate_visitor_data(data)
        if not is_valid:
            logging.warning(
                f"[数据校验] 字段 [{failed_field}] 不合规 — phone='{phone}' plate='{plate}'"
            )
            # 保存已知信息供纠错 session 使用
            self._correction_context = {
                "name": name, "phone": phone, "plate": plate,
                "company": company, "reason": reason,
                "invalid_field": failed_field,
            }
            # 向前端推送简短文字提示（纠错 session 通过 SayHello 播放纠错语音）
            hint = "手机号格式不对，稍后重新确认。" if failed_field == "phone" \
                   else "车牌格式不对，稍后重新确认。"
            try:
                await client_ws.send_json({"type": "retract_ai_text"})
                await client_ws.send_json({"type": "ai_text",  "text": hint})
                await client_ws.send_json({"type": "ai_end"})
            except Exception as pe:
                logging.error(f"[数据校验] 推送提示失败: {pe}")
            # 信号 session 重启
            self._session_exit_reason = "validation_failed"
            return

        # ── 校验通过：入库 + 企业微信通知 ───────────────────────────────
        # _save_to_db 和月统计均含同步 sqlite3.connect，offload 到线程池
        await asyncio.to_thread(self._save_to_db, name, phone, plate, company, reason)

        def _query_month_count(user_uuid: str) -> int:
            conn = sqlite3.connect(DB_FILE)
            cur  = conn.cursor()
            cur.execute(
                "SELECT COUNT(*) FROM visits WHERE user_uuid = ? "
                "AND strftime('%Y-%m', timestamp) = strftime('%Y-%m', 'now')",
                (user_uuid,)
            )
            count = cur.fetchone()[0]
            conn.close()
            return count

        month_count = await asyncio.to_thread(_query_month_count, self.user_uuid)

        success = await send_visitor_notification(
            name, plate, phone, company, reason,
            f"提示: 该访客本月已来访 {month_count} 次"
        )
        if success:
            logging.info("✅ 访客信息已入库并推送微信")
        else:
            logging.error("❌ 微信推送失败")

        self.visit_recorded      = True
        self._correction_context = None   # 清空纠错状态

        try:
            await client_ws.send_json({"status": "completed"})
        except Exception:
            pass
        self._session_exit_reason = "completed"

    # ─────────────────────────────────────────────────────────────────────────
    # 辅助方法
    # ─────────────────────────────────────────────────────────────────────────
    def _extract_partial_info(self) -> dict:
        buf = self.llm_response_buffer
        if "===JSON_BEGIN===" in buf and "===JSON_END===" in buf:
            s = buf.find("===JSON_BEGIN===") + len("===JSON_BEGIN===")
            e = buf.find("===JSON_END===")
            try:
                return json.loads(buf[s:e].strip())
            except Exception:
                pass
        return {}

    HUMAN_INTENT_KEYWORDS = [
        "叫人", "转人工", "找人工", "要人工", "人工服务", "要真人", "找真人",
        "让人来", "叫保安", "找保安", "算了", "不用了", "不想说了", "帮我叫",
        "不知道", "不清楚", "我不会", "叫个人来", "有没有人"
    ]
    MAX_TURNS_BEFORE_HUMAN = 8

    async def _check_human_intent(self, user_text: str, client_ws: WebSocket = None):
        if self.human_notified or self.visit_recorded:
            return
        has_kw      = any(kw in user_text for kw in self.HUMAN_INTENT_KEYWORDS)
        has_timeout = self.turn_count >= self.MAX_TURNS_BEFORE_HUMAN
        if not has_kw and not has_timeout:
            return

        self.human_notified = True
        partial = self._extract_partial_info()
        if self._correction_context:
            for k in ("name", "phone", "plate", "company", "reason"):
                if not partial.get(k):
                    partial[k] = self._correction_context.get(k, "")

        if not partial.get("phone") or not partial.get("plate"):
            try:
                def _fetch_user_sync(user_uuid: str) -> dict:
                    conn = sqlite3.connect(DB_FILE)
                    conn.row_factory = sqlite3.Row
                    cur  = conn.cursor()
                    cur.execute("SELECT * FROM users WHERE uuid = ?", (user_uuid,))
                    row  = cur.fetchone()
                    conn.close()
                    return dict(row) if row else {}

                row = await asyncio.to_thread(_fetch_user_sync, self.user_uuid)
                if row:
                    for k, col in [("name","name"), ("phone","phone"),
                                   ("plate","default_plate"), ("company","default_company")]:
                        if not partial.get(k) and row.get(col):
                            partial[k] = row[col]
            except Exception as e:
                logging.error(f"人工通知时读取 DB 失败: {e}")

        trigger = (
            f"用户主动请求人工（原话：{user_text}）"
            if has_kw else
            f"对话已进行 {self.turn_count} 轮，信息仍未采集完整"
        )
        logging.info(f"[人工协助] 触发，原因: {trigger}")
        await send_human_required_notification(
            partial.get("name",""), partial.get("plate",""),
            partial.get("phone",""), partial.get("company",""),
            partial.get("reason",""), trigger
        )
        try:
            def _insert_pending_case(params: tuple) -> None:
                conn = sqlite3.connect(DB_FILE)
                cur  = conn.cursor()
                cur.execute(
                    """INSERT INTO pending_human_cases
                       (user_uuid, partial_name, partial_phone, partial_plate,
                        partial_company, partial_reason, trigger_reason)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    params
                )
                conn.commit()
                conn.close()

            await asyncio.to_thread(
                _insert_pending_case,
                (self.user_uuid,
                 partial.get("name",""), partial.get("phone",""),
                 partial.get("plate",""), partial.get("company",""),
                 partial.get("reason",""), trigger)
            )
        except Exception as e:
            logging.error(f"保存 pending_human_cases 失败: {e}")

        if client_ws:
            try:
                await client_ws.send_json({"type": "hang_up"})
                await client_ws.close()
            except Exception:
                pass

    def _save_to_db(self, name, phone, plate, company, reason):
        try:
            conn = sqlite3.connect(DB_FILE)
            cur  = conn.cursor()
            cur.execute(
                """INSERT INTO users (uuid, phone, name, default_plate, default_company)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(uuid) DO UPDATE SET
                       phone=excluded.phone,
                       name=excluded.name,
                       default_plate=excluded.default_plate,
                       default_company=excluded.default_company""",
                (self.user_uuid, phone, name, plate, company)
            )
            cur.execute(
                "INSERT INTO visits (user_uuid, visit_reason) VALUES (?, ?)",
                (self.user_uuid, reason)
            )
            conn.commit()
            conn.close()
            logging.info(f"✅ 入库完成: {name} ({plate})")
        except Exception as e:
            logging.error(f"入库失败: {e}")
