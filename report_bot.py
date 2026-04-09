import asyncio
import aiohttp
import logging
from datetime import datetime, timedelta, timezone

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
        async with session.get(url, params=params or {}, timeout=aiohttp.ClientTimeout(total=20)) as r:
            return await r.json()
    except Exception as e:
        logging.error(f"Bitrix error {method}: {e}")
        return {}


async def bx_all(session, method, params=None):
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
        if len(result) < 50:
            break
        start += 50
        await asyncio.sleep(0.25)
    return all_results


async def tg_send(session, text):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    try:
        async with session.post(url, json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}) as r:
            await r.json()
    except Exception as e:
        logging.error(f"TG error: {e}")


async def collect_stats(session):
    now = datetime.now(MSK)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow_start = today_start + timedelta(days=1)

    date_from = today_start.strftime("%Y-%m-%d 00:00:00")
    date_to   = tomorrow_start.strftime("%Y-%m-%d 00:00:00")

    all_deals = await bx_all(session, "crm.deal.list", {
        "filter[>=DATE_CREATE]": date_from,
        "filter[<DATE_CREATE]": date_to,
        "select[]": ["ID", "SOURCE_ID"],
    })

    sources = {}
    for d in all_deals:
        src = d.get("SOURCE_ID") or "нет"
        sources[src] = sources.get(src, 0) + 1

    avito_count = (
        sources.get("CALL", 0) +
        sources.get("AVITO", 0) +
        sources.get("AVITO_COMAGIC", 0) +
        sources.get("UC_Y6UT3Y", 0)
    )
    garage_count = sources.get("UC_98W3GU", 0)

    planned = await bx_all(session, "crm.deal.list", {
        f"filter[>={MEETING_PLANNED_FIELD}]": today_start.strftime("%Y-%m-%dT00:00:00+03:00"),
        f"filter[<{MEETING_PLANNED_FIELD}]": tomorrow_start.strftime("%Y-%m-%dT00:00:00+03:00"),
        "select[]": ["ID"],
    })

    completed = await bx_all(session, "crm.deal.list", {
        f"filter[>={MEETING_FACT_FIELD}]": today_start.strftime("%Y-%m-%dT00:00:00+03:00"),
        f"filter[<{MEETING_FACT_FIELD}]": tomorrow_start.strftime("%Y-%m-%dT00:00:00+03:00"),
        "select[]": ["ID"],
    })

    logging.info(f"Итог на {now.strftime('%H:%M')}: Авито={avito_count} Гараж={garage_count} Назначено={len(planned)} Состоялось={len(completed)}")

    return {
        "avito": avito_count,
        "garage": garage_count,
        "planned": len(planned),
        "completed": len(completed),
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
    logging.info("Отчёт отправлен")


async def wait_until_next_hour():
    """Ждём до следующего круглого часа МСК"""
    now = datetime.now(MSK)
    next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    wait_seconds = (next_hour - now).total_seconds()
    logging.info(f"Следующий отчёт в {next_hour.strftime('%H:%M')} МСК (через {int(wait_seconds)} сек)")
    await asyncio.sleep(wait_seconds)


async def main():
    logging.info("Бот запущен v32 — отчёт каждый час в HH:00 МСК")

    async with aiohttp.ClientSession() as session:
        # Первый отчёт сразу при запуске
        await send_report(session)

        # Дальше — каждый круглый час
        while True:
            await wait_until_next_hour()
            await send_report(session)


if __name__ == "__main__":
    asyncio.run(main())
