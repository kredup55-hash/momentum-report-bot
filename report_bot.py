import asyncio
import aiohttp
import logging
from datetime import datetime, timedelta, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

TG_TOKEN = "7624139134:AAFHFI9HzjHHz-qq-MmZK1Y7SDlLc0UdFuk"
TG_CHAT_ID = -1003723824906
BITRIX_WEBHOOK = "https://momentum-techit.bitrix24.ru/rest/2790/56agqwjf3rysukb8/"

MSK = timezone(timedelta(hours=3))


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
    for attempt in range(1, 6):
        try:
            logging.info(f"Попытка отправки отчёта #{attempt}")
            async with session.post(url, json={
                "chat_id": TG_CHAT_ID,
                "text": text
            }, timeout=aiohttp.ClientTimeout(total=15)) as r:
                result = await r.json()
                if result.get("ok"):
                    logging.info(f"✅ Отчёт успешно отправлен с попытки {attempt}")
                    return True
        except Exception as e:
            logging.error(f"Попытка {attempt} не удалась: {e}")
        await asyncio.sleep(3)
    logging.error("❌ Не удалось отправить отчёт после 5 попыток")
    return False


async def collect_stats(session):
    now = datetime.now(MSK)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow_start = today_start + timedelta(days=1)

    date_from = today_start.strftime("%Y-%m-%d 00:00:00")
    date_to = tomorrow_start.strftime("%Y-%m-%d 00:00:00")

    logging.info(f"Сбор данных за день {today_start.strftime('%d.%m')}")

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

    return {
        "avito": avito_count,
        "garage": garage_count,
        "time": now.strftime("%H:%M"),
        "date": today_start.strftime("%d.%m.%Y"),
    }


async def send_report(session):
    stats = await collect_stats(session)
    text = f"""Отчёт Моментум — {stats['date']}
Накоплено за день (на {stats['time']} МСК)
────────────────────
Контакты с Авито: {stats['avito']}
Лиды с гаража: {stats['garage']}
────────────────────
Встречи назначены сегодня: {stats.get('planned', 0)}
Состоялось встреч: {stats.get('completed', 0)}
"""

    await tg_send(session, text)


async def main():
    logging.info("Бот запущен v35 — стабильная версия")

    async with aiohttp.ClientSession() as session:
        await send_report(session)

        while True:
            await asyncio.sleep(3600)
            await send_report(session)


if __name__ == "__main__":
    asyncio.run(main())
