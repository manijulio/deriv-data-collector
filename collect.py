import asyncio, websockets, json, pandas as pd
from datetime import datetime
import nest_asyncio
nest_asyncio.apply()

async def fetch_batch(instrument, end, count=5000):
    url = "wss://ws.derivws.com/websockets/v3?app_id=1"
    req = {"ticks_history": instrument, "end": end, "count": count, "style": "ticks"}
    async with websockets.connect(url) as ws:
        await ws.send(json.dumps(req))
        resp = await ws.recv()
    return json.loads(resp)

async def collect_backfill(instrument="1HZ75V", num_batches=200):
    all_rows = []
    end = "latest"
    for i in range(num_batches):
        print("Batch " + str(i+1) + "/" + str(num_batches))
        data = await fetch_batch(instrument, end)
        if "history" not in data:
            print("Erreur API : " + str(data))
            break
        times  = data["history"]["times"]
        prices = data["history"]["prices"]
        batch  = sorted(zip(times, prices), key=lambda x: x[0])
        all_rows = batch + all_rows
        end = batch[0][0] - 1
        await asyncio.sleep(1.5)
    df = pd.DataFrame(all_rows, columns=["unix_time", "price"])
    df["price"]      = df["price"].astype(float)
    df["timestamp"]  = pd.to_datetime(df["unix_time"], unit="s")
    df["instrument"] = instrument
    df["tick_size"]  = df["price"].diff().abs()
    df["direction"]  = df["price"].diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    df["session_id"] = "BACKFILL_AUTO"
    df = df[["timestamp","unix_time","instrument","price","direction","tick_size","session_id"]]
    df = df.sort_values("unix_time").reset_index(drop=True)
    return df

df = asyncio.run(collect_backfill())
df = df.drop_duplicates(subset=["unix_time"]).sort_values("unix_time").reset_index(drop=True)
filename = "data/1HZ75V_" + datetime.now().strftime("%Y%m%d") + ".csv"
df.to_csv(filename, index=False)
print("Fichier exporte : " + filename + " — " + str(len(df)) + " ticks")
