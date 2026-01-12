#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import re
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup


URL_RIOS = "https://www.redhidrosurmedioambiente.es/saih/resumen/rios"
RSS_PATH = Path("rss.xml")
STATE_PATH = Path("state.json")

# Ajusta estos umbrales a tu gusto (metros de NIVEL MEDIO).
# Si tú ya tenías umbrales por estación, lo suyo es migrarlos aquí.
THRESHOLDS = {
    1: 1.0,
    2: 2.0,
    3: 3.0,
}

RSS_TITLE = "AVISOS CRECIDAS SAIH"
RSS_LINK = URL_RIOS
RSS_DESC = "Avisos automáticos de crecidas (SAIH Hidrosur) cuando el NIVEL MEDIO cruza niveles 1/2/3 en estaciones (MA/CA)."


def parse_float_es(x: str):
    if not x:
        return None
    x = x.strip()
    if x.lower() == "n/d":
        return None
    # 0,93 -> 0.93
    x = x.replace(".", "").replace(",", ".")
    try:
        return float(x)
    except ValueError:
        return None


def rfc2822_now():
    return datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S %z")


def safe_load_state():
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {
        "last_levels": {},          # {station_id: last_level_float}
        "last_heartbeat_date": "",  # "YYYY-MM-DD"
    }


def save_state(state: dict):
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def fetch_html():
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; RSSBot/1.0; +https://github.com/lloviznas/saih-alertas)"
    }
    r = requests.get(URL_RIOS, headers=headers, timeout=30)
    r.raise_for_status()
    return r.text


def extract_last_update(html: str):
    # En el footer sale: "Datos actualizados a: 12-01-2026 13:00:00"
    m = re.search(r"Datos actualizados a:\s*([0-9]{2}-[0-9]{2}-[0-9]{4}\s+[0-9]{2}:[0-9]{2}:[0-9]{2})", html)
    if not m:
        return None
    try:
        # Interpretamos como hora local de la web (normalmente UTC o local); lo guardamos como texto.
        return m.group(1)
    except Exception:
        return None


def parse_rios_table(html: str):
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        return []

    rows = table.find_all("tr")
    if not rows:
        return []

    stations = []
    for tr in rows[1:]:  # saltar cabecera
        tds = tr.find_all("td")
        if len(tds) < 3:
            continue

        # Estructura típica:
        # 0 Número
        # 1 Nombre
        # 2 Nivel Medio (m)
        # 3 Caudal Medio
        # ...
        station_id = tds[0].get_text(strip=True)
        name = tds[1].get_text(" ", strip=True)
        level_txt = tds[2].get_text(strip=True)

        # Algunos tienen provincia al final "(MA)" "(CA)" etc dentro del nombre
        prov = None
        mprov = re.search(r"\(([A-Z]{2})\)\s*$", name)
        if mprov:
            prov = mprov.group(1)

        level = parse_float_es(level_txt)

        stations.append({
            "id": station_id,
            "name": name,
            "prov": prov,
            "level": level,
        })

    return stations


def crossed(prev, curr, threshold):
    if prev is None or curr is None:
        return False
    return prev < threshold <= curr


def build_items(state, stations, last_update_text):
    items = []
    last_levels = state.get("last_levels", {})

    # Solo MA/CA según tu descripción
    stations = [s for s in stations if s.get("prov") in ("MA", "CA")]

    for s in stations:
        sid = s["id"]
        curr = s["level"]
        prev = last_levels.get(sid, None)

        # Detectar cruces de nivel 1/2/3
        for lvl, thr in THRESHOLDS.items():
            if crossed(prev, curr, thr):
                title = f"Nivel {lvl} alcanzado: {s['name']}"
                desc = f"Estación {sid} ({s.get('prov')}): NIVEL MEDIO sube de {prev:.2f} m a {curr:.2f} m y cruza {thr:.2f} m. Datos actualizados a: {last_update_text or 'n/d'}."
                guid = f"cross-{sid}-L{lvl}-{last_update_text or rfc2822_now()}"
                pub = rfc2822_now()
                items.append({"title": title, "description": desc, "guid": guid, "pubDate": pub, "link": RSS_LINK})

        # Actuali
