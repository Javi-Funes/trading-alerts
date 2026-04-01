import os
import pandas as pd
import requests
import yfinance as yf
from datetime import datetime

SHEET_URL   = os.getenv("SHEET_URL")
TOKEN       = os.getenv("TELEGRAM_TOKEN")
CHAT_ID     = os.getenv("CHAT_ID")
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
# DATA912
# Campo de precio: "c" (close)
# ─────────────────────────────────────────
endpoint_cache = {}

def fetch_endpoint(endpoint):
    if endpoint in endpoint_cache:
        return endpoint_cache[endpoint]

    url = f"https://data912.com/{endpoint}" if endpoint.startswith("live/") \
          else f"https://data912.com/api/{endpoint}"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict):
            data = list(data.values())
        endpoint_cache[endpoint] = {"data": data, "ok": True}
    except Exception as e:
        print(f"[Data912 error] endpoint={endpoint} → {e}")
        endpoint_cache[endpoint] = {"data": [], "ok": False, "error": str(e)}

    return endpoint_cache[endpoint]


def get_price_data912(row):
    endpoint = str(row.get("Endpoint", "")).strip()
    ticker   = str(row.get("Ticker", "")).strip().upper()

    if not endpoint or not ticker:
        return None, "sin endpoint configurado"

    result = fetch_endpoint(endpoint)

    if not result["ok"]:
        return None, f"endpoint caído: {result.get('error', 'error desconocido')}"

    for item in result["data"]:
        symbol = str(item.get("symbol") or item.get("ticker") or "").upper()
        if symbol == ticker:
            precio = item.get("c") or item.get("price") or item.get("last")
            if precio is not None:
                return float(precio), None
            return None, "precio vacío en la respuesta"

    return None, f"ticker no encontrado en {endpoint}"


# ─────────────────────────────────────────
# YFINANCE — fallback solo si Es_USA = YES
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

    precio, motivo = None, None

    if fuente == "DATA912":
        precio, motivo = get_price_data912(row)
    elif fuente == "YF":
        precio = get_price_yf(ticker)
        motivo = "yfinance sin datos" if precio is None else None

    # Fallback a yfinance solo para activos USA
    if precio is None and es_usa == "YES":
        print(f"[Fallback yfinance] {ticker}")
        precio = get_price_yf(ticker)
        if precio is not None:
            motivo = None

    return precio, motivo


# ─────────────────────────────────────────
# ACTUALIZAR GOOGLE SHEETS
# ─────────────────────────────────────────
def update_sheet(row_id, max_precio, estado):
    if not WEBHOOK_URL:
        return
    payload = {"id": row_id, "max_precio": max_precio, "estado": estado}
    try:
        requests.post(WEBHOOK_URL, json=payload, timeout=10)
    except Exception as e:
        print(f"[Webhook error] {e}")


# ─────────────────────────────────────────
# DETECCIÓN DE PRECIOS CONGELADOS
# Si el precio no cambió respecto al Max Precio anterior
# y llevan más de N ejecuciones iguales → puede estar tildado
# ─────────────────────────────────────────
def precio_congelado(precio_actual, max_precio_anterior, pct_change):
    """
    Detecta si el precio parece congelado.
    Criterio: pct_change = 0.0 exacto puede indicar datos viejos.
    No es 100% confiable (puede ser un día sin movimiento real),
    pero sirve como alerta temprana.
    """
    return pct_change == 0.0


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
def main():
    hora = datetime.now().strftime("%H:%M")

    if not SHEET_URL:
        print("ERROR: SHEET_URL no configurada")
        return

    try:
        df = pd.read_csv(SHEET_URL)
    except Exception as e:
        send_msg(f"⚠️ ERROR CRÍTICO\nNo se pudo leer la hoja de posiciones.\nError: {e}")
        return

    print(f"Procesando {len(df)} filas... ({hora})")

    # ── Tracking de errores por endpoint ──
    errores_endpoint = {}   # endpoint → lista de tickers con error
    alertas_enviadas = 0

    for _, row in df.iterrows():
        estado_actual = str(row.get("Estado", "")).strip().upper()
        if estado_actual != "OPEN":
            continue

        row_id   = str(row.get("ID", "")).strip()
        ticker   = str(row.get("Ticker", "")).strip()
        endpoint = str(row.get("Endpoint", "")).strip()

        precio, motivo_error = get_price(row)

        # ── Sin precio: registrar error ────────────────────────
        if precio is None:
            print(f"[Sin precio] {ticker} — {motivo_error}")
            if endpoint not in errores_endpoint:
                errores_endpoint[endpoint] = []
            errores_endpoint[endpoint].append(f"{ticker} ({motivo_error})")
            continue

        print(f"{ticker} → ${precio:.2f}")

        # ── Trailing Stop ──────────────────────────────────────
        try:
            max_precio_anterior = float(row.get("Max Precio", precio))
        except (ValueError, TypeError):
            max_precio_anterior = precio
        if max_precio_anterior == 0:
            max_precio_anterior = precio

        max_precio  = max(max_precio_anterior, precio)
        trail_pct   = float(row.get("Trail %", 0) or 0)
        trailing_sl = max_precio * (1 - trail_pct / 100) if trail_pct > 0 else None

        try:
            tp = float(row.get("TP", 0) or 0)
            sl = float(row.get("SL", 0) or 0)
        except (ValueError, TypeError):
            tp = sl = 0

        nuevo_estado = "OPEN"

        # TP → alerta, sigue abierto
        if tp > 0 and precio >= tp:
            msg = (f"📈 {ticker} → OBJETIVO TP ALCANZADO\n"
                   f"Precio: {precio:.2f} | TP: {tp:.2f}\n"
                   f"Trailing SL activo: {trailing_sl:.2f} (max: {max_precio:.2f})\n"
                   f"Posición SIGUE ABIERTA — el trailing cuida tus ganancias")
            send_msg(msg)
            alertas_enviadas += 1

        # SL fijo → cierra
        if sl > 0 and precio <= sl:
            msg = (f"🛑 {ticker} → STOP LOSS activado\n"
                   f"Precio: {precio:.2f} | SL: {sl:.2f}")
            send_msg(msg)
            nuevo_estado = "CLOSED"
            alertas_enviadas += 1

        # Trailing → cierra
        elif trailing_sl is not None and precio <= trailing_sl:
            msg = (f"🔁 {ticker} → TRAILING STOP activado\n"
                   f"Precio: {precio:.2f} | Trail SL: {trailing_sl:.2f}\n"
                   f"Max: {max_precio:.2f} | Trail: {trail_pct}%")
            send_msg(msg)
            nuevo_estado = "CLOSED"
            alertas_enviadas += 1

        update_sheet(row_id, round(max_precio, 4), nuevo_estado)

    # ── Alertas de endpoints con errores ──────────────────────
    if errores_endpoint:
        lineas = ["⚠️ ALERTA DE DATOS — algunos precios no pudieron obtenerse\n"]
        for ep, tickers in errores_endpoint.items():
            ep_nombre = ep if ep else "sin endpoint"
            lineas.append(f"Endpoint: {ep_nombre}")
            for t in tickers:
                lineas.append(f"  • {t}")
        lineas.append(f"\nHora: {hora}")
        lineas.append("Acción: verificá Data912 o revisá los tickers en la hoja.")
        send_msg("\n".join(lineas))

    # ── Resumen si no hubo ninguna alerta de trading ──────────
    posiciones_open = len([r for _, r in df.iterrows()
                           if str(r.get("Estado","")).strip().upper() == "OPEN"])
    if alertas_enviadas == 0 and not errores_endpoint and posiciones_open > 0:
        print(f"Sin disparos. {posiciones_open} posiciones monitoreadas.")
    elif not errores_endpoint:
        print(f"Ejecución completada. {alertas_enviadas} alerta(s) enviada(s).")

    print("Ejecución completada.")


if __name__ == "__main__":
    main()
