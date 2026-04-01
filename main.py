import os
import pandas as pd
import requests
import yfinance as yf

SHEET_URL = os.getenv("SHEET_URL")
TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")

# -------- TELEGRAM --------
def send_msg(msg):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    requests.post(url, data={"chat_id": CHAT_ID, "text": msg})

# -------- DATA912 --------
def fetch_data912():
    try:
        url = "https://data912.com/api/stocks"
        return requests.get(url).json()
    except:
        return []

def get_price_data912(data, ticker):
    for item in data:
        if item.get("symbol") == ticker:
            return item.get("price")
    return None

# -------- YFINANCE --------
def get_price_yf(ticker):
    try:
        data = yf.Ticker(ticker).history(period="1d")
        if data.empty:
            return None
        return float(data["Close"].iloc[-1])
    except:
        return None

# -------- UPDATE SHEET --------
def update_sheet(id, max_precio, estado):
    payload = {
        "id": id,
        "max_precio": max_precio,
        "estado": estado
    }
    try:
        requests.post(WEBHOOK_URL, json=payload)
    except:
        pass

# -------- GET PRICE --------
def get_price(row, data912):
    precio = None

    if row["Fuente"] == "DATA912":
        precio = get_price_data912(data912, row["Ticker"])
    else:
        precio = get_price_yf(row["Ticker"])

    # fallback USA
    if precio is None and row["Es_USA"] == "YES":
        precio = get_price_yf(row["Ticker"])

    return precio

# -------- MAIN --------
df = pd.read_csv(SHEET_URL)
data912 = fetch_data912()

for _, row in df.iterrows():
    if row["Estado"] != "OPEN":
        continue

    id = row["ID"]
    ticker = row["Ticker"]

    precio = get_price(row, data912)

    if precio is None:
        send_msg(f"{ticker} sin precio")
        continue

    max_precio = max(row["Max Precio"], precio)
    trailing_sl = max_precio * (1 - row["Trail %"]/100)

    estado = "OPEN"

    if precio >= row["TP"]:
        send_msg(f"{ticker} → TAKE PROFIT {precio}")
        estado = "CLOSED"

    elif precio <= row["SL"]:
        send_msg(f"{ticker} → STOP LOSS {precio}")
        estado = "CLOSED"

    elif precio <= trailing_sl:
        send_msg(f"{ticker} → TRAILING STOP {precio}")
        estado = "CLOSED"

    update_sheet(id, max_precio, estado)
