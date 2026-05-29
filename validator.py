"""
访客数据清洗与校验模块
在大模型输出的结构化 JSON 写入 SQLite 之前进行强制拦截。

校验规则：
- phone：必须为 1 开头、第二位 3-9、共 11 位纯数字（中国大陆手机号）
- plate：必须符合中国大陆标准车牌格式（普通蓝牌 / 新能源绿牌）

校验失败时返回面向 LLM 的 System Prompt 纠错指令，由调用方注入至下一轮对话。
"""

import re
import logging

# ---------------------------------------------------------------------------
# 手机号正则：1[3-9] 开头，共 11 位数字
# ---------------------------------------------------------------------------
_PHONE_RE = re.compile(r'^1[3-9]\d{9}$')

# ---------------------------------------------------------------------------
# 车牌正则（中国大陆）
# 省份汉字缩写
_PROVINCES = '京津沪渝冀豫云辽黑湘皖鲁新苏浙赣鄂桂甘晋蒙陕吉闽贵粤川青藏琼宁夏'
# 字母集（排除 I / O，防止与数字 1/0 混淆）
_LETTERS = 'A-HJ-NP-Z'
# 普通蓝牌：省份 + 城市字母 + 5位（字母/数字/特殊尾标）
_PLATE_NORMAL = rf'[{_PROVINCES}][{_LETTERS}][{_LETTERS}0-9]{{4}}[{_LETTERS}0-9挂学警港澳]'
# 新能源绿牌：省份 + 城市字母 + D/F 标识 + 5位字母/数字（共7位）
_PLATE_NEV    = rf'[{_PROVINCES}][{_LETTERS}][DF][{_LETTERS}0-9]{{5}}'
_PLATE_RE = re.compile(rf'^(?:{_PLATE_NORMAL}|{_PLATE_NEV})$')

# ---------------------------------------------------------------------------

def validate_phone(phone: str) -> tuple[bool, str]:
    """
    校验手机号。
    返回 (is_valid, error_hint)。
    error_hint 为空字符串表示校验通过。
    """
    if not phone or not phone.strip():
        return False, '访客未提供手机号'
    digits = re.sub(r'\D', '', phone.strip())
    if _PHONE_RE.match(digits):
        return True, ''
    n = len(digits)
    if n < 11:
        hint = f'您提供的手机号仅有 {n} 位，不足 11 位，请引导访客重新播报完整的 11 位手机号'
    elif n > 11:
        hint = f'您提供的手机号共 {n} 位，超过 11 位，请引导访客重新确认手机号'
    else:
        hint = '您提供的手机号首位或格式不正确（须以 1[3-9] 开头），请引导访客重新播报'
    return False, hint


def validate_plate(plate: str) -> tuple[bool, str]:
    """
    校验中国大陆车牌（普通蓝牌 / 新能源绿牌）。
    返回 (is_valid, error_hint)。
    """
    if not plate or not plate.strip():
        return False, '访客未提供车牌号'
    plate_clean = plate.strip().upper()
    if _PLATE_RE.match(plate_clean):
        return True, ''
    hint = (
        f'您提供的车牌 "{plate}" 不符合中国大陆标准车牌格式，'
        f'请引导访客重新播报完整车牌号（示例格式：沪A12345 或 粤BD12345F）'
    )
    return False, hint


def validate_visitor_data(data: dict) -> tuple[bool, str, str]:
    """
    对大模型输出的访客信息字典进行严格校验。

    参数：
        data  —— 从 LLM JSON 提取的访客字段字典，含 phone、plate 等键

    返回：
        (is_valid, failed_field, correction_system_prompt)
        - is_valid              : True 表示全部通过，可以入库
        - failed_field          : 校验失败的字段名（'phone' / 'plate' / ''）
        - correction_system_prompt : 供注入下一轮对话的最高优先级 System Prompt；
                                     校验通过时为空字符串
    """
    phone = data.get('phone', '') or ''
    plate = data.get('plate', '') or ''

    # --- 手机号校验 ---
    phone_ok, phone_hint = validate_phone(phone)
    if not phone_ok:
        logging.warning(f'[数据校验] phone 校验失败: "{phone}" → {phone_hint}')
        system_prompt = _build_correction_prompt('phone', phone, phone_hint)
        return False, 'phone', system_prompt

    # --- 车牌校验 ---
    plate_ok, plate_hint = validate_plate(plate)
    if not plate_ok:
        logging.warning(f'[数据校验] plate 校验失败: "{plate}" → {plate_hint}')
        system_prompt = _build_correction_prompt('plate', plate, plate_hint)
        return False, 'plate', system_prompt

    return True, '', ''


def _build_correction_prompt(field: str, raw_value: str, hint: str) -> str:
    """
    构造注入 LLM 的最高优先级纠错 System Prompt。
    该指令将覆盖原有对话规则，强制 AI 主导修正流程。
    """
    return (
        f'【⚠️ 系统数据校验拦截 — 最高优先级指令，必须立即执行】\n'
        f'后台校验系统发现访客提供的【{field}】字段内容 "{raw_value}" 未通过格式校验，'
        f'已被强制拦截，禁止入库。\n'
        f'具体原因：{hint}。\n'
        f'你现在必须：\n'
        f'1. 用简短口语告知访客信息有误（一句话，不超过15字）；\n'
        f'2. 引导访客重新清晰播报该项信息；\n'
        f'3. 待访客重新提供并确认后，重新收集完整四项信息并输出 JSON。\n'
        f'在访客提供正确信息之前，绝对禁止再次输出 JSON。'
    )
