import asyncio
import aiohttp
import logging
from datetime import datetime, timezone, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

TG_TOKEN = "7624139134:AAFHFI9HzjHHz-qq-MmZK1Y7SDlLc0UdFuk"
TG_CHAT_ID = -1003723824906
BITRIX_WEBHOOK = "https://momentum-techit.bitrix24.ru/rest/2790/56agqwjf3rysukb8/"

MSK = timezone(timedelta(hours=3))

SOURCE_AVITO = "AVITO"
SOURCE_GARAGE = "UC_98W3GU"
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

    # Пробуем разные форматы дат для DATE_CREATE
    fmt1 = (today_msk - timedelta(hours=3)).strftime("%Y-%m-%dT%H:%M:%S")  # UTC без зоны
    fmt2 = today_msk.strftime("%Y-%m-%dT%H:%M:%S+03:00")  # МСК с зоной
    fmt3 = today_msk.strftime("%d.%m.%Y")  # Просто дата

    logging.info(f"Тест форматов: UTC={fmt1}, MSK={fmt2}, Date={fmt3}")

    # Тест 1 — UTC без зоны
    r1 = await bx(session, "crm.deal.list", {
        "filter[>=DATE_CREATE]": fmt1,
        "filter[SOURCE_ID]": SOURCE_AVITO,
        "select[]": ["ID", "DATE_CREATE"],
    })
    logging.info(f"Авито UTC без зоны: {len(r1) if isinstance(r1, list) else 0}")
    if isinstance(r1, list) and r1:
        logging.info(f"  Пример DATE_CREATE: {r1[0].get('DATE_CREATE')}")

    # Тест 2 — МСК с зоной
    r2 = await bx(session, "crm.deal.list", {
        "filter[>=DATE_CREATE]": fmt2,
        "filter[SOURCE_ID]": SOURCE_AVITO,
        "select[]": ["ID", "DATE_CREATE"],
    })
    logging.info(f"Авито МСК+03:00: {len(r2) if isinstance(r2, list) else 0}")

    # Тест 3 — просто дата
    r3 = await bx(session, "crm.deal.list", {
        "filter[>=DATE_CREATE]": fmt3,
        "filter[SOURCE_ID]": SOURCE_AVITO,
        "select[]": ["ID", "DATE_CREATE"],
    })
    logging.info(f"Авито DD.MM.YYYY: {len(r3) if isinstance(r3, list) else 0}")
    if isinstance(r3, list) and r3:
        logging.info(f"  Пример DATE_CREATE: {r3[0].get('DATE_CREATE')}")

    # Тест 4 — все сделки за сегодня без фильтра источника
    r4 = await bx(session, "crm.deal.list", {
        "filter[>=DATE_CREATE]": fmt3,
        "select[]": ["ID", "SOURCE_ID", "DATE_CREATE"],
    })
    logging.info(f"Все сделки DD.MM.YYYY: {len(r4) if isinstance(r4, list) else 0}")
    sources = {}
    if isinstance(r4, list):
        for d in r4:
            src = d.get("SOURCE_ID", "")
            sources[src] = sources.get(src, 0) + 1
    logging.info(f"  По источникам: {sources}")

    # Встречи
    today_msk_str = today_msk.strftime("%Y-%m-%dT%H:%M:%S+03:00")
    today_msk_end_str = (today_msk + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S+03:00")

    planned = await bx(session, "crm.deal.list", {
        f"filter[>={MEETING_PLANNED_FIELD}]": today_msk_str,
        f"filter[<{MEETING_PLANNED_FIELD}]": today_msk_end_str,
        "select[]": ["ID"],
    })
    planned_count = len(planned) if isinstance(planned, list) else 0

    completed = await bx(session, "crm.deal.list", {
        f"filter[>={MEETING_FACT_FIELD}]": today_msk_str,
        f"filter[<{MEETING_FACT_FIELD}]": today_msk_end_str,
        "select[]": ["ID"],
    })
    completed_count = len(completed) if isinstance(completed, list) else 0

    # Используем лучший результат для Авито
    avito_count = max(len(r1) if isinstance(r1, list) else 0,
                      len(r2) if isinstance(r2, list) else 0,
                      len(r3) if isinstance(r3, list) else 0)

    garage = await bx(session, "crm.deal.list", {
        "filter[>=DATE_CREATE]": fmt3,
        "filter[SOURCE_ID]": SOURCE_GARAGE,
        "select[]": ["ID"],
    })
    garage_count = len(garage) if isinstance(garage, list) else 0

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
    logging.info("Бот запущен v11")
    async with aiohttp.ClientSession() as session:
        await send_report(session)
        while True:
            await asyncio.sleep(3600)
            await send_report(session)


if __name__ == "__main__":
    asyncio.run(main())
