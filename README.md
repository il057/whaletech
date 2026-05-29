# 智能园区门卫系统

## 架构

```
访客端（浏览器）
    │  WebSocket 实时音频流
    ▼
FastAPI 服务 (main.py)
    ├── VoicePipeline ──► 火山引擎实时对话 API（ASR + TTS + LLM）
    │       └── 对话完成后写入 SQLite（users / visits）
    │
    ├── 微信机器人长轮询 ──► ilink WeChat Bot API
    │       └── 保安发消息 → Text-to-SQL → DeepSeek LLM → 回复查询结果
    │
    └── SQLite (data.db)
            ├── users  (uuid, name, phone, default_plate, default_company)
            └── visits (id, user_uuid, visit_reason, timestamp)
```

## 部署步骤

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 配置环境变量（见下方说明）
cp .env.example .env  # 按需编辑

# 3. 启动服务（首次运行会在终端打印微信扫码登录二维码）
python main.py
```

服务默认监听 `http://0.0.0.0:8000`，访客通过 `http://<IP>:8000` 访问前端页面。

## 环境变量

| 变量名 | 说明 | 必填 |
|---|---|---|
| `VOLC_APP_ID` | 火山引擎语音应用 ID | ✅ |
| `VOLC_ACCESS_TOKEN` | 火山引擎语音访问令牌 | ✅ |
| `ARK_API_KEY` | 火山方舟 LLM API Key（用于保安 Text-to-SQL 查询） | ✅ |

> 微信登录 Token 在首次运行时扫码授权，自动持久化至 `.wechat_token.json`，无需手动配置。
