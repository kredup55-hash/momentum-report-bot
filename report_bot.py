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
SOURCE_AVITO = "AVITO_COMAGIC"
SOURCE_GARAGE = "UC_98W3GU"

# Стадии воронки "Аренда" (id=18)
# Пригласили в офис — нужно уточнить STAGE_ID
# Пришел в офис — нужно уточнить STAGE_ID
STAGE_INVITED = "C18:UC_EVNSVS"   # Пригласили в офис
STAGE_CAME    = "C18:UC_L3PZAL"   # Пришел в офис


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
    today_utc = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(hours=3)
    today_str = today_utc.strftime("%Y-%m-%dT%H:%M:%S")

    # Контакты с Авито
    avito_deals = await bx(session, "crm.deal.list", {
        "filter[>=DATE_CREATE]": today_str,
        "filter[SOURCE_ID]": SOURCE_AVITO,
        "select[]": ["ID"],
    })
    avito_count = len(avito_deals) if isinstance(avito_deals, list) else 0

    # Лиды с гаража
    garage_deals = await bx(session, "crm.deal.list", {
        "filter[>=DATE_CREATE]": today_str,
        "filter[SOURCE_ID]": SOURCE_GARAGE,
        "select[]": ["ID"],
    })
    garage_count = len(garage_deals) if isinstance(garage_deals, list) else 0

    # Встречи назначены — сделки перешли в стадию "Пригласили в офис" сегодня
    invited = await bx(session, "crm.deal.list", {
        "filter[>=DATE_MODIFY]": today_str,
        "filter[STAGE_ID]": STAGE_INVITED,
        "select[]": ["ID", "TITLE", "STAGE_ID"],
    })
    planned_count = len(invited) if isinstance(invited, list) else 0

    # Состоялось встреч — сделки в стадии "Пришел в офис" изменены сегодня
    came = await bx(session, "crm.deal.list", {
        "filter[>=DATE_MODIFY]": today_str,
        "filter[STAGE_ID]": STAGE_CAME,
        "select[]": ["ID", "TITLE", "STAGE_ID"],
    })
    completed_count = len(came) if isinstance(came, list) else 0

    # Дебаг стадий воронки Аренда
    stages_arenда = await bx(session, "crm.dealcategory.stages", {"id": 18})
    if isinstance(stages_arenда, list):
        for s in stages_arenда:
            logging.info(f"Стадия Аренда: {s.get('STATUS_ID')} → {s.get('NAME')}")

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
    logging.info("Бот запущен v4")
    async with aiohttp.ClientSession() as session:
        await send_report(session)
        while True:
            await asyncio.sleep(3600)
            await send_report(session)


if __name__ == "__main__":
    asyncio.run(main())
