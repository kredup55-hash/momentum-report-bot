import asyncio
import aiohttp
import logging
from datetime import datetime, timezone, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

TG_TOKEN = "7624139134:AAFHFI9HzjHHz-qq-MmZK1Y7SDlLc0UdFuk"
TG_CHAT_ID = -1003723824906
BITRIX_WEBHOOK = "https://momentum-techit.bitrix24.ru/rest/2790/56agqwjf3rysukb8/"

MSK = timezone(timedelta(hours=3))
MEETING_PLANNED_FIELD = "UF_CRM_1756299008904"
MEETING_FACT_FIELD = "UF_CRM_1756299040214"


async def bx(session, method, params=None):
    url = f"{BITRIX_WEBHOOK}{method}.json"
    try:
        async with session.get(url, params=params or {}, timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json()
            return data
    except Exception as e:
        logging.error(f"Bitrix error {method}: {e}")
        return {}


async def bx_all(session, method, params=None):
    """Получаем все записи с пагинацией"""
    all_results = []
    start = 0
    while True:
        p = dict(params or {})
        p["start"] = start
        data = await bx(session, method, p)
        result = data.get("result", [])
        if not result:
            break
        all_results.extend(result)
        total = data.get("total", 0)
        if len(all_results) >= total:
            break
        start += 50
        await asyncio.sleep(0.2)
    return all_results


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
    today_utc_str = (today_msk - timedelta(hours=3)).strftime("%Y-%m-%dT%H:%M:%S")
    today_utc_end = (today_msk - timedelta(hours=3) + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S")
    today_msk_str = today_msk.strftime("%Y-%m-%dT%H:%M:%S+03:00")
    today_msk_end = (today_msk + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S+03:00")

    # Все сделки за сегодня с пагинацией
    all_deals = await bx_all(session, "crm.deal.list", {
        "filter[>=DATE_CREATE]": today_utc_str,
        "filter[<DATE_CREATE]": today_utc_end,
        "select[]": ["ID", "SOURCE_ID"],
    })

    sources = {}
    for d in all_deals:
        src = d.get("SOURCE_ID", "нет")
        sources[src] = sources.get(src, 0) + 1

    logging.info(f"Всего сделок: {len(all_deals)} | Источники: {sources}")

    avito_count = sources.get("AVITO", 0) + sources.get("AVITO_COMAGIC", 0) + sources.get("CALL", 0) + sources.get("UC_Y6UT3Y", 0)
    garage_count = sources.get("UC_98W3GU", 0)

    # Встречи с пагинацией
    planned = await bx_all(session, "crm.deal.list", {
        f"filter[>={MEETING_PLANNED_FIELD}]": today_msk_str,
        f"filter[<{MEETING_PLANNED_FIELD}]": today_msk_end,
        "select[]": ["ID"],
    })
    planned_count = len(planned)

    completed = await bx_all(session, "crm.deal.list", {
        f"filter[>={MEETING_FACT_FIELD}]": today_msk_str,
        f"filter[<{MEETING_FACT_FIELD}]": today_msk_end,
        "select[]": ["ID"],
    })
    completed_count = len(completed)

    logging.info(f"Итог: Авито={avito_count} Гараж={garage_count} Встречи={planned_count} Состоялось={completed_count}")

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
    logging.info("Бот запущен v17")
    async with aiohttp.ClientSession() as session:
        await send_report(session)
        while True:
            await asyncio.sleep(3600)
            await send_report(session)


if __name__ == "__main__":
    asyncio.run(main())
