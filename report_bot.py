=import asyncio
import aiohttp
import logging
from datetime import datetime, timezone, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

TG_TOKEN = "7624139134:AAFHFI9HzjHHz-qq-MmZK1Y7SDlLc0UdFuk"
TG_CHAT_ID = -1003723824906
BITRIX_WEBHOOK = "https://momentum-techit.bitrix24.ru/rest/2790/56agqwjf3rysukb8/"

MSK = timezone(timedelta(hours=3))

SOURCE_GARAGE = "UC_98W3GU"
AVITO_SOURCES = ["AVITO", "AVITO_COMAGIC"]

MEETING_PLANNED_FIELD = "UF_CRM_1756299008904"
MEETING_FACT_FIELD = "UF_CRM_1756299040214"


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
    today_msk = now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_utc = today_msk
    today_str = today_msk.strftime("%Y-%m-%dT%H:%M:%S+03:00")
    today_end = (today_msk + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S+03:00")

    # Контакты с Авито — считаем все Авито источники
    avito_count = 0
    for src in AVITO_SOURCES:
        deals = await bx(session, "crm.deal.list", {
            "filter[>=DATE_CREATE]": today_str,
            "filter[SOURCE_ID]": src,
            "select[]": ["ID"],
        })
        cnt = len(deals) if isinstance(deals, list) else 0
        logging.info(f"Авито источник {src}: {cnt}")
        avito_count += cnt

    # Лиды с гаража
    garage_deals = await bx(session, "crm.deal.list", {
        "filter[>=DATE_CREATE]": today_str,
        "filter[SOURCE_ID]": SOURCE_GARAGE,
        "select[]": ["ID"],
    })
    garage_count = len(garage_deals) if isinstance(garage_deals, list) else 0

    # Встречи назначены сегодня
    planned = await bx(session, "crm.deal.list", {
        f"filter[>={MEETING_PLANNED_FIELD}]": today_str,
        f"filter[<{MEETING_PLANNED_FIELD}]": today_end,
        "select[]": ["ID", MEETING_PLANNED_FIELD],
    })
    planned_count = len(planned) if isinstance(planned, list) else 0

    # Состоялось встреч — дебаг
    completed = await bx(session, "crm.deal.list", {
        f"filter[>={MEETING_FACT_FIELD}]": today_str,
        f"filter[<{MEETING_FACT_FIELD}]": today_end,
        "select[]": ["ID", MEETING_FACT_FIELD],
    })
    completed_count = len(completed) if isinstance(completed, list) else 0
    logging.info(f"Фактические встречи примеры: {completed[:3] if isinstance(completed, list) else []}")

    logging.info(f"Авито={avito_count} Гараж={garage_count} Встречи={planned_count} Состоялось={completed_count}")

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
    logging.info("Бот запущен v9")
    async with aiohttp.ClientSession() as session:
        await send_report(session)
        while True:
            await asyncio.sleep(3600)
            await send_report(session)


if __name__ == "__main__":
    asyncio.run(main())
