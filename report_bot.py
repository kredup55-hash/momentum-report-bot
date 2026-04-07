import asyncio
import aiohttp
import logging
from datetime import datetime, timezone, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

TG_TOKEN = "7624139134:AAFHFI9HzjHHz-qq-MmZK1Y7SDlLc0UdFuk"
TG_CHAT_ID = -1003723824906
BITRIX_WEBHOOK = "https://momentum-techit.bitrix24.ru/rest/2790/56agqwjf3rysukb8/"

MSK = timezone(timedelta(hours=3))

# Источники
SOURCE_AVITO = "AVITO"         # Контакты с Авито
SOURCE_GARAGE = "UC_GARAGE"    # Яндекс Гараж — уточним ID после первого запуска


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


async def get_sources(session):
    """Получаем все источники для дебага"""
    sources = await bx(session, "crm.status.list", {"filter[ENTITY_ID]": "SOURCE"})
    logging.info(f"Источники: {sources}")
    return sources


async def collect_stats(session):
    now = datetime.now(MSK)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_str = today_start.strftime("%Y-%m-%dT%H:%M:%S+03:00")

    # 1. Контакты с Авито (сделки с источником Авито за сегодня)
    avito_deals = await bx(session, "crm.deal.list", {
        "filter[>=DATE_CREATE]": today_str,
        "filter[SOURCE_ID]": "AVITO",
        "select[]": ["ID", "SOURCE_ID", "DATE_CREATE"],
    })
    avito_count = len(avito_deals) if isinstance(avito_deals, list) else 0

    # Попробуем также лиды с Авито
    avito_leads = await bx(session, "crm.lead.list", {
        "filter[>=DATE_CREATE]": today_str,
        "filter[SOURCE_ID]": "AVITO",
        "select[]": ["ID", "SOURCE_ID"],
    })
    avito_count += len(avito_leads) if isinstance(avito_leads, list) else 0

    # 2. Лиды с Гаража (сделки и лиды с источником Яндекс Гараж)
    garage_deals = await bx(session, "crm.deal.list", {
        "filter[>=DATE_CREATE]": today_str,
        "filter[SOURCE_ID]": "UC_YANDEX_GARAGE",
        "select[]": ["ID", "SOURCE_ID"],
    })
    garage_count = len(garage_deals) if isinstance(garage_deals, list) else 0

    garage_leads = await bx(session, "crm.lead.list", {
        "filter[>=DATE_CREATE]": today_str,
        "filter[SOURCE_ID]": "UC_YANDEX_GARAGE",
        "select[]": ["ID", "SOURCE_ID"],
    })
    garage_count += len(garage_leads) if isinstance(garage_leads, list) else 0

    # 3. Встречи назначены сегодня (тип 2 = встреча, созданы сегодня)
    meetings_planned = await bx(session, "crm.activity.list", {
        "filter[>=CREATED]": today_str,
        "filter[TYPE_ID]": 2,
        "select[]": ["ID", "TYPE_ID", "COMPLETED", "CREATED", "DEADLINE"],
    })
    planned_count = len(meetings_planned) if isinstance(meetings_planned, list) else 0

    # 4. Встречи состоялись (завершены сегодня)
    completed_count = 0
    if isinstance(meetings_planned, list):
        for m in meetings_planned:
            if m.get("COMPLETED") == "Y":
                completed_count += 1

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
    logging.info("Бот запущен")
    async with aiohttp.ClientSession() as session:
        # Первый запуск — показываем источники для проверки
        await get_sources(session)

        # Отправляем первый отчёт сразу
        await send_report(session)

        # Затем каждый час
        while True:
            await asyncio.sleep(3600)
            await send_report(session)


if __name__ == "__main__":
    asyncio.run(main())
