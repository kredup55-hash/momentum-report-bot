import asyncio
import aiohttp
import logging
from datetime import datetime, timezone, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

TG_TOKEN = "7624139134:AAFHFI9HzjHHz-qq-MmZK1Y7SDlLc0UdFuk"
TG_CHAT_ID = -1003723824906
BITRIX_WEBHOOK = "https://momentum-techit.bitrix24.ru/rest/2790/56agqwjf3rysukb8/"

MSK = timezone(timedelta(hours=3))


async def bx(session, method, params=None):
    url = f"{BITRIX_WEBHOOK}{method}.json"
    try:
        async with session.get(url, params=params or {}, timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json()
            return data.get("result", [])
    except Exception as e:
        logging.error(f"Bitrix error {method}: {e}")
        return []


async def tg_send(session, text):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    try:
        async with session.post(url, json={
            "chat_id": TG_CHAT_ID,
            "text": text,
            "parse_mode": "HTML"
        }) as r:
            result = await r.json()
            if not result.get("ok"):
                logging.error(f"TG send error: {result}")
    except Exception as e:
        logging.error(f"TG error: {e}")


async def collect_stats(session):
    now = datetime.now(MSK)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_str = today_start.strftime("%Y-%m-%dT%H:%M:%S")

    # Все сделки за сегодня
    all_deals = await bx(session, "crm.deal.list", {
        "filter[>=DATE_CREATE]": today_str,
        "select[]": ["ID", "SOURCE_ID", "TITLE", "STAGE_ID"],
    })

    avito_count = 0
    garage_count = 0

    if isinstance(all_deals, list):
        for d in all_deals:
            src = str(d.get("SOURCE_ID", ""))
            title = str(d.get("TITLE", "")).lower()
            logging.info(f"Сделка: {d.get('TITLE')} | SOURCE_ID={src}")
            if src in ("AVITO", "avito") or "avito" in title:
                avito_count += 1
            if "GARAGE" in src.upper() or "гараж" in title:
                garage_count += 1

    # Встречи назначены сегодня — стадия "Пригласили в офис" изменена сегодня
    invited_deals = await bx(session, "crm.deal.list", {
        "filter[>=DATE_MODIFY]": today_str,
        "filter[STAGE_ID]": "C5:UC_EVNSVS",
        "select[]": ["ID", "TITLE", "STAGE_ID", "DATE_MODIFY"],
    })
    planned_count = len(invited_deals) if isinstance(invited_deals, list) else 0
    logging.info(f"Пригласили в офис: {planned_count}")

    # Состоялось встреч — стадия "Пришел в офис" изменена сегодня
    came_deals = await bx(session, "crm.deal.list", {
        "filter[>=DATE_MODIFY]": today_str,
        "filter[STAGE_ID]": "C5:UC_L3PZAL",
        "select[]": ["ID", "TITLE", "STAGE_ID", "DATE_MODIFY"],
    })
    completed_count = len(came_deals) if isinstance(came_deals, list) else 0
    logging.info(f"Пришел в офис: {completed_count}")

    # Дебаг — все стадии
    stages = await bx(session, "crm.dealcategory.stages", {"id": 5})
    logging.info(f"Стадии воронки: {stages}")

    return {
        "avito": avito_count,
        "garage": garage_count,
        "planned": planned_count,
        "completed": completed_count,
        "time": now.strftime("%H:%M"),
        "date": now.strftime("%d.%m.%Y"),
    }


async def send_report(session):
    stats = await collect_stats(session)
    text = (
        f"📊 <b>Отчёт Моментум</b> — {stats['date']}\n"
        f"🕐 Накоплено за день (на {stats['time']} МСК)\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📱 Контакты с Авито: <b>{stats['avito']}</b>\n"
        f"🚗 Лиды с гаража: <b>{stats['garage']}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📅 Встречи назначены сегодня: <b>{stats['planned']}</b>\n"
        f"✅ Состоялось встреч: <b>{stats['completed']}</b>"
    )
    await tg_send(session, text)
    logging.info(f"Отчёт отправлен: {stats}")


async def main():
    logging.info("Бот запущен v2")
    async with aiohttp.ClientSession() as session:
        await send_report(session)
        while True:
            await asyncio.sleep(3600)
            await send_report(session)


if __name__ == "__main__":
    asyncio.run(main())
