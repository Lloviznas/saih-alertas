import json
from datetime import datetime, timezone
from email.utils import format_datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ======================
# CONFIG
# ======================
URL = "https://www.redhidrosurmedioambiente.es/saih/resumen/rios"
STATE_FILE = Path("state.json")
RSS_FILE = Path("rss.xml")

HEADERS = {
    "User-Agent": "saih-rss-monitor/1.0 (personal use)"
}

# Solo trabajamos con Málaga y Cádiz (por texto en el nombre)
ONLY_TAGS = ("(MA)", "(CA)")

# Anti-rebote (histeresis) para evitar spam por oscilaciones mínimas
HYSTERESIS = 0.05  # metros

# Umbrales por estación (en metros)
# Formato: "NOMBRE EXACTO": [nivel1, nivel2, nivel3]
THRESHOLDS_BY_NAME = {
    "AZUD DE PAREDONES (MA)": [3.0, 4.0, 5.0],
    "BARCA DE LA FLORIDA (CA)": [2.0, 3.0, 4.0],
    "GUADALETE-JEREZ DE LA FRONTERA (CA)": [4.0, 5.0, 6.0],
    "JUNTA DE LOS RÍOS (CA)": [3.0, 4.0, 5.0],
    "PUENTE DE CUADRO OJOS-UBRIQUE (CA)": [1.0, 2.0, 3.0],
    "RIO GUADALHORCE (ALJAIMA) (MA)": [1.0, 1.5, 2.0],
    "RIO GUADALTEBA (AFORO TEBA) (MA)": [1.5, 2.0, 2.5],
    "RIO ALAMO (BENALUP-CASAS V.) (CA)": [4.5, 5.5, 6.5],
    "RIO BENAMARGOSA (S. NEGRO) (MA)": [1.7, 2.3, 2.8],
    "RIO CAMPANILLAS (LOS LLANES) (MA)": [1.0, 2.0, 3.2],
    "RIO GENAL (JUBRIQUE) (MA)": [1.0, 1.6, 2.1],
    "RIO GUADALHORCE (ARCHIDONA) (MA)": [2.0, 3.0, 5.0],
    "RIO GUADALHORCE (BOBADILLA) (MA)": [2.5, 3.0, 4.0],
    "RIO GUADALHORCE (CARTAMA) (MA)": [2.5, 3.5, 4.5],
    "RIO GUADIARO (TR.MAJACEITE) (MA)": [1.4, 1.7, 2.0],
    "RIO GUADIARO (S PABLO BUCEITE) (CA)": [3.0, 4.0, 5.0],
    "RIO HOZGARGANTA (JIMENA) (CA)": [2.0, 3.0, 4.0],
    "RIO TURON (ARDALES) (MA)": [1.5, 2.0, 2.5],
}

# ======================
# Helpers
# ======================
def normalize_name(s: str) -> str:
    """Normaliza espacios para evitar fallos por dobles espacios."""
    return " ".join((s or "").strip().split())

THRESHOLDS_BY_NAME_NORM = {normalize_name(k): v for k, v in THRESHOLDS_BY_NAME.items()}

def parse_float_es(value: str):
    """
    Convierte '0,23' -> 0.23.
    Si es 'n/d' o vacío, devuelve None.
    Si viniera con unidades tipo '3,2 m', extrae el número.
    """
    if not value:
        return None
    v = value.strip().lower()
    if v in ("n/d", "nd", "-", "—"):
        return None

    import re
    m = re.search(r"[-+]?\d+(?:[.,]\d+)?", v)
    if not m:
        return None

    num = m.group(0).replace(".", "").replace(",", ".")
    try:
        return float(num)
    except ValueError:
        return None

def load_state():
    """
    state.json guarda:
    {
      "last_level_alerted": {
        "<station_id>": 0..3
      }
    }
    """
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            # Si se corrompe el JSON, reiniciamos sin romper todo
            return {"last_level_alerted": {}}
    return {"last_level_alerted": {}}

def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

def fetch_rows():
    r = requests.get(URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    table = soup.find("table")
    if not table:
        raise RuntimeError("No encuentro la tabla en la página. Puede haber cambiado el HTML.")

    rows = []
    for tr in table.find_all("tr"):
        tds = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
        if len(tds) < 3:
            continue

        joined = " ".join(tds).lower()
        # Heurística para saltar cabecera
        if "nivel medio" in joined and "nombre" in joined:
            continue

        station_id = tds[0].strip()
        name = normalize_name(tds[1])
        nivel_raw = tds[2].strip()

        if not any(tag in name for tag in ONLY_TAGS):
            continue

        nivel = parse_float_es(nivel_raw)

        rows.append({
            "id": station_id,
            "name": name,
            "nivel": nivel,
            "nivel_raw": nivel_raw,
        })

    return rows

def build_rss(items):
    now = datetime.now(timezone.utc)
    pub_date = format_datetime(now)

    parts = []
    parts.append('<?xml version="1.0" encoding="UTF-8"?>')
    parts.append('<rss version="2.0">')
    parts.append("<channel>")
    parts.append("<title>AVISOS CRECIDAS SAIH</title>")
    parts.append(f"<link>{URL}</link>")
    parts.append("<description>Avisos automáticos de crecidas (SAIH Hidrosur) cuando el NIVEL MEDIO cruza niveles 1/2/3 en estaciones (MA/CA).</description>")
    parts.append(f"<lastBuildDate>{pub_date}</lastBuildDate>")

    for it in items[:50]:
        parts.append("<item>")
        parts.append(f"<title><![CDATA[{it['title']}]]></title>")
        parts.append(f"<link>{it['link']}</link>")
        parts.append(f"<guid isPermaLink='false'>{it['guid']}</guid>")
        parts.append(f"<pubDate>{it['pubDate']}</pubDate>")
        parts.append(f"<description><![CDATA[{it['description']}]]></description>")
        parts.append("</item>")

    parts.append("</channel>")
    parts.append("</rss>")
    return "\n".join(parts)

def compute_reached_level(nivel: float, thresholds: list[float]) -> int:
    """Nivel alcanzado por subida (sin histeresis)."""
    reached = 0
    if nivel >= thresholds[0]:
        reached = 1
    if nivel >= thresholds[1]:
        reached = 2
    if nivel >= thresholds[2]:
        reached = 3
    return reached

def compute_rearm_level(nivel: float, thresholds: list[float], prev_level: int) -> int:
    """
    Rearme por bajada con histeresis:
    - Si estabas en nivel 3 y baja < (t3 - H), baja a 2
    - Si estabas en nivel 2 y baja < (t2 - H), baja a 1
    - Si estabas en nivel 1 y baja < (t1 - H), baja a 0
    """
    lvl = prev_level

    if lvl >= 3 and nivel < (thresholds[2] - HYSTERESIS):
        lvl = 2
    if lvl >= 2 and nivel < (thresholds[1] - HYSTERESIS):
        lvl = 1
    if lvl >= 1 and nivel < (thresholds[0] - HYSTERESIS):
        lvl = 0

    return lvl

def main():
    state = load_state()
    last_level_alerted = state.get("last_level_alerted", {})  # {station_id: 0..3}

    rows = fetch_rows()

    now = datetime.now(timezone.utc)
    now_rfc2822 = format_datetime(now)

    alerts = []

    for r in rows:
        station_id = r["id"]
        name = r["name"]
        nivel = r["nivel"]

        if nivel is None:
            continue

        thresholds = THRESHOLDS_BY_NAME_NORM.get(name)
        if not thresholds:
            continue  # estación no configurada

        prev_level = int(last_level_alerted.get(station_id, 0))

        # 1) Rearme por bajada
        rearmed_level = compute_rearm_level(nivel, thresholds, prev_level)
        if rearmed_level != prev_level:
            last_level_alerted[station_id] = rearmed_level
            prev_level = rearmed_level

        # 2) Nivel alcanzado por subida
        reached_level = compute_reached_level(nivel, thresholds)

        # 3) Alertas por niveles nuevos alcanzados
        if reached_level > prev_level:
            for lvl in range(prev_level + 1, reached_level + 1):
                thr = thresholds[lvl - 1]
                title = f"ALERTA NIVEL {lvl}: {name} NIVEL MEDIO={nivel:.2f} m (umbral {thr:.2f} m)"
                desc = (
                    f"<p><b>Estación:</b> {name}</p>"
                    f"<p><b>NIVEL MEDIO (m):</b> {nivel:.2f}</p>"
                    f"<p><b>Nivel alcanzado:</b> {lvl}</p>"
                    f"<p><b>Umbral nivel {lvl}:</b> {thr:.2f} m</p>"
                    f"<p><b>Histeresis anti-rebote:</b> {HYSTERESIS:.2f} m</p>"
                    f"<p><i>Fuente:</i> {URL}</p>"
                )
                alerts.append({
                    "title": title,
                    "link": URL,
                    "guid": f"{station_id}-L{lvl}-{int(now.timestamp())}",
                    "description": desc,
                    "pubDate": now_rfc2822,
                })

            last_level_alerted[station_id] = reached_level

    state["last_level_alerted"] = last_level_alerted
    save_state(state)

    rss = build_rss(alerts)
    RSS_FILE.write_text(rss, encoding="utf-8")

    print(f"OK. Alertas generadas: {len(alerts)}")

if __name__ == "__main__":
    main()