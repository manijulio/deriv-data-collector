import asyncio, websockets, json, pandas as pd
from datetime import datetime
import nest_asyncio
nest_asyncio.apply()

INSTRUMENTS = [
    "R_10", "R_25", "R_75", "R_100",
    "BOOM300N", "BOOM500", "BOOM600", "BOOM900", "BOOM1000",
    "CRASH300N", "CRASH500", "CRASH600", "CRASH900", "CRASH1000",
    "stpRNG", "frxXAUUSD"
]

async def fetch_batch(instrument, end, count=5000):
    url = "wss://ws.derivws.com/websockets/v3?app_id=1"
    req = {"ticks_history": instrument, "end": end, "count": count, "style": "ticks"}
    async with websockets.connect(url) as ws:
        await ws.send(json.dumps(req))
        resp = await ws.recv()
    return json.loads(resp)

async def collect_one(instrument):
    all_rows = []
    end = "latest"
    for i in range(200):
        print(instrument + " - Batch " + str(i+1))
        data = await fetch_batch(instrument, end)
        if "history" not in data:
            code = data.get("error", {}).get("code", "inconnu")
            print(instrument + " - Arret : " + str(code))
            break
        times  = data["history"]["times"]
        prices = data["history"]["prices"]
        batch  = sorted(zip(times, prices), key=lambda x: x[0])
        all_rows = batch + all_rows
        end = batch[0][0] - 1
        await asyncio.sleep(1.5)
    if not all_rows:
        print(instrument + " - Aucune donnee collectee")
        return
    df = pd.DataFrame(all_rows, columns=["unix_time", "price"])
    df["price"]      = df["price"].astype(float)
    df["timestamp"]  = pd.to_datetime(df["unix_time"], unit="s")
    df["instrument"] = instrument
    df["tick_size"]  = df["price"].diff().abs()
    df["direction"]  = df["price"].diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    df["session_id"] = "BACKFILL_AUTO"
    df = df[["timestamp","unix_time","instrument","price","direction","tick_size","session_id"]]
    df = df.drop_duplicates(subset=["unix_time"]).sort_values("unix_time").reset_index(drop=True)
    filename = "data/" + instrument + "_" + datetime.now().strftime("%Y%m%d") + ".csv"
    df.to_csv(filename, index=False)
    print(instrument + " - OK : " + str(len(df)) + " ticks -> " + filename)

async def main():
    for instrument in INSTRUMENTS:
        await collect_one(instrument)

asyncio.run(main())
