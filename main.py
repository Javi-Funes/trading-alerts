import os
import pandas as pd
import requests
import yfinance as yf

SHEET_URL = os.getenv("SHEET_URL")
TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")

# ─────────────────────────────────────────
# TELEGRAM
# ─────────────────────────────────────────
def send_msg(msg):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    try:
        requests.post(url, data={"chat_id": CHAT_ID, "text": msg}, timeout=10)
    except Exception as e:
        print(f"[Telegram error] {e}")


# ─────────────────────────────────────────
# DATA912 — cache por endpoint
# IMPORTANTE: el campo de precio en la API es "c" (close), NO "price"
# ─────────────────────────────────────────
endpoint_cache = {}

def fetch_endpoint(endpoint):
    """
    Soporta endpoints con ruta completa:
      live/usa_stocks  → https://data912.com/live/usa_stocks
      stocks           → https://data912.com/api/stocks
    """
    if endpoint in endpoint_cache:
        return endpoint_cache[endpoint]

    if endpoint.startswith("live/"):
        url = f"https://data912.com/{endpoint}"
    else:
        url = f"https://data912.com/api/{endpoint}"

    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        # Normalizar: si viene como dict lo convertimos a lista
        if isinstance(data, dict):
            data = list(data.values())
        endpoint_cache[endpoint] = data
    except Exception as e:
        print(f"[Data912 error] endpoint={endpoint} → {e}")
        endpoint_cache[endpoint] = []

    return endpoint_cache[endpoint]


def get_price_data912(row):
    """
    Busca el precio de un activo en Data912.
    Campos intentados para el símbolo: symbol, ticker
    Campo de precio: "c" (close) — campo real de la API
    """
    endpoint = str(row.get("Endpoint", "")).strip()
    ticker = str(row.get("Ticker", "")).strip().upper()

    if not endpoint or not ticker:
        return None

    data = fetch_endpoint(endpoint)

    for item in data:
        symbol = str(item.get("symbol") or item.get("ticker") or "").upper()
        if symbol == ticker:
            # El campo de precio es "c" (close), no "price"
            precio = item.get("c") or item.get("price") or item.get("last")
            if precio is not None:
                return float(precio)

    return None


# ─────────────────────────────────────────
# YFINANCE — solo activos USA
# ─────────────────────────────────────────
def get_price_yf(ticker):
    try:
        data = yf.Ticker(ticker).history(period="1d")
        if data.empty:
            return None
        return float(data["Close"].iloc[-1])
    except Exception as e:
        print(f"[yfinance error] {ticker} → {e}")
        return None


# ─────────────────────────────────────────
# ROUTER DE PRECIOS
# ─────────────────────────────────────────
def get_price(row):
    fuente = str(row.get("Fuente", "")).strip().upper()
    es_usa = str(row.get("Es_USA", "")).strip().upper()
    ticker = str(row.get("Ticker", "")).strip()

    precio = None

    if fuente == "DATA912":
        precio = get_price_data912(row)
    elif fuente == "YF":
        precio = get_price_yf(ticker)

    # Fallback a yfinance solo para activos USA
    if precio is None and es_usa == "YES":
        print(f"[Fallback yfinance] {ticker}")
        precio = get_price_yf(ticker)

    return precio


# ─────────────────────────────────────────
# ACTUALIZAR GOOGLE SHEETS VÍA APPS SCRIPT
# ─────────────────────────────────────────
def update_sheet(row_id, max_precio, estado):
    if not WEBHOOK_URL:
        return
    payload = {
        "id": row_id,
        "max_precio": max_precio,
        "estado": estado
    }
    try:
        requests.post(WEBHOOK_URL, json=payload, timeout=10)
    except Exception as e:
        print(f"[Webhook error] {e}")


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
def main():
    if not SHEET_URL:
        print("ERROR: SHEET_URL no configurada")
        return

    try:
        df = pd.read_csv(SHEET_URL)
    except Exception as e:
        send_msg(f"⚠️ No se pudo leer la hoja: {e}")
        return

    print(f"Procesando {len(df)} filas...")

    for _, row in df.iterrows():
        estado_actual = str(row.get("Estado", "")).strip().upper()
        if estado_actual != "OPEN":
            continue

        row_id  = str(row.get("ID", "")).strip()
        ticker  = str(row.get("Ticker", "")).strip()

        precio = get_price(row)

        if precio is None:
            msg = f"⚠️ {ticker} ({row_id}): sin precio disponible"
            print(msg)
            send_msg(msg)
            continue

        print(f"{ticker} → precio actual: {precio}")

        # ── Trailing Stop ──────────────────────────────
        try:
            max_precio_anterior = float(row.get("Max Precio", precio))
        except (ValueError, TypeError):
            max_precio_anterior = precio

        max_precio = max(max_precio_anterior, precio)

        try:
            trail_pct = float(row.get("Trail %", 0))
        except (ValueError, TypeError):
            trail_pct = 0

        trailing_sl = max_precio * (1 - trail_pct / 100) if trail_pct > 0 else None

        # ── Evaluar condiciones (prioridad: TP > SL > Trailing) ──
        nuevo_estado = "OPEN"

        try:
            tp = float(row.get("TP", 0))
            sl = float(row.get("SL", 0))
        except (ValueError, TypeError):
            tp = 0
            sl = 0

        if tp > 0 and precio >= tp:
            msg = f"✅ {ticker} → TAKE PROFIT alcanzado\nPrecio: {precio:.2f} | TP: {tp:.2f}"
            send_msg(msg)
            nuevo_estado = "CLOSED"

        elif sl > 0 and precio <= sl:
            msg = f"🛑 {ticker} → STOP LOSS activado\nPrecio: {precio:.2f} | SL: {sl:.2f}"
            send_msg(msg)
            nuevo_estado = "CLOSED"

        elif trailing_sl is not None and precio <= trailing_sl:
            msg = (
                f"🔁 {ticker} → TRAILING STOP activado\n"
                f"Precio: {precio:.2f} | Trail SL: {trailing_sl:.2f} "
                f"(Max: {max_precio:.2f}, Trail: {trail_pct}%)"
            )
            send_msg(msg)
            nuevo_estado = "CLOSED"

        # ── Persistir estado en Google Sheets ─────────
        update_sheet(row_id, round(max_precio, 4), nuevo_estado)

    print("Ejecución completada.")


if __name__ == "__main__":
    main()
