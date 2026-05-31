# 智能园区门卫系统

## 架构

```mermaid
graph TD
    subgraph Client ["终端交互层"]
        C1["访客手机端 (H5)"]
        C2["门卫端 (普通微信)"]
        C3["物业管理员 (PC/移动端)"]
    end

    subgraph Backend ["核心服务层 (FastAPI)"]
        B1["WebSocket 语音路由"]
        B2["iLink 长轮询客户端"]
        B3["Admin 可视化路由"]
    end

    subgraph AI_Engine ["AI 处理引擎"]
        P1["VoicePipeline 对话编排器"]
        A1["火山引擎实时对话 API"]
        A2["DeepSeek / NLP & SQL"]
    end

    subgraph Storage ["数据持久层"]
        DB[("SQLite 本地数据库")]
    end

    %% 访客链路
    C1 -- "WebRTC 音频流 / WebSocket 信令" --> B1
    B1 --> P1
    P1 <--> A1
    P1 -- "解析并强校验 JSON -> 入库" --> DB

    %% 保安微信链路
    B2 -- "主动推送：来访提醒/人工/日报" --> C2
    C2 -- "回复：自然语言查询/补录" --> B2
    B2 --> A2
    A2 -- "生成受限 SQL / 结构化数据" --> DB

    %% 物业后台链路
    C3 -- "HTTP Basic Auth 验证" --> B3
    B3 -- "执行统计查询 / 生成 CSV 导出" --> DB
```

## 部署步骤

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 配置环境变量
cp .env.example .env   # 编辑 .env，填入各项 Key

# 3. 启动服务（首次运行在终端打印微信扫码二维码）
python main.py
```

服务监听 `http://0.0.0.0:8000`。访客页面：`/`，物业后台：`/admin`。

公网暴露可用 cloudflared：`cloudflared tunnel --url http://localhost:8000`

## 环境变量

| 变量名 | 说明 | 必填 |
|---|---|---|
| `VOLC_APP_ID` | 火山引擎语音应用 ID | ✅ |
| `VOLC_ACCESS_TOKEN` | 火山引擎语音访问令牌 | ✅ |
| `ARK_API_KEY` | 火山方舟 LLM API Key（保安 Text-to-SQL / 日报生成） | ✅ |
| `ADMIN_PASSWORD` | 物业后台登录密码 | ✅ |
| `ADMIN_URL` | 后台完整 URL（供微信机器人告知门卫，可选） | ❌ |
| `DAILY_REPORT_HOUR` | 每日简报推送小时（默认 `18`） | ❌ |

> 微信登录 Token 首次运行时扫码授权，自动持久化至 `.wechat_token.json`，重启后自动恢复。
