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
        "last_levels": {},
    }


def save_state(state: dict):
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def fetch_html():
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; RSSBot/1.0)"
    }
    r = requests.get(URL_RIOS, headers=headers, timeout=30)
    r.raise_for_status()
    return r.text


def extract_last_update(html: str):
    m = re.search(
        r"Datos actualizados a:\s*([0-9]{2}-[0-9]{2}-[0-9]{4}\s+[0-9]{2}:[0-9]{2}:[0-9]{2})",
        html,
    )
    return m.group(1) if m else "n/d"


def parse_rios_table(html: str):
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        return []

    rows = table.find_all("tr")
    stations = []

    for tr in rows[1:]:
        tds = tr.find_all("td")
        if len(tds) < 3:
            continue

        station_id = tds[0].get_text(strip=True)
        name = tds[1].get_text(" ", strip=True)
        level_txt = tds[2].get_text(strip=True)

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

    stations = [s for s in stations if s.get("prov") in ("MA", "CA")]

    for s in stations:
        sid = s["id"]
        curr = s["level"]
        prev = last_levels.get(sid)

        for lvl, thr in THRESHOLDS.items():
            if crossed(prev, curr, thr):
                title = f"Nivel {lvl} alcanzado: {s['name']}"
                desc = (
                    f"Estación {sid} ({s.get('prov')}): "
                    f"NIVEL MEDIO pasa de {prev:.2f} m a {curr:.2f} m "
                    f"y cruza {thr:.2f} m. "
                    f"Datos actualizados a: {last_update_text}."
                )
                guid = f"cross-{sid}-L{lvl}-{last_update_text}"
                items.append({
                    "title": title,
                    "description": desc,
                    "guid": guid,
                    "pubDate": rfc2822_now(),
                    "link": RSS_LINK,
                })

        if curr is not None:
            last_levels[sid] = curr

    # 🔥 HEARTBEAT FORZADO: SIEMPRE al menos 1 item
    if not items:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        items.append({
            "title": "Sin cruces detectados (heartbeat)",
            "description": (
                "No se han detectado cruces de umbral (niveles 1/2/3) "
                f"en estaciones MA/CA. Datos actualizados a: {last_update_text}."
            ),
            "guid": f"heartbeat-{today}",
            "pubDate": rfc2822_now(),
            "link": RSS_LINK,
        })

    state["last_levels"] = last_levels
    return items


def write_rss(items):
    parts = []
    parts.append('<?xml version="1.0" encoding="UTF-8"?>')
    parts.append('<rss version="2.0">')
    parts.append("<channel>")
    parts.append(f"<title>{RSS_TITLE}</title>")
    parts.append(f"<link>{RSS_LINK}</link>")
    parts.append(f"<description>{RSS_DESC}</description>")
    parts.append(f"<lastBuildDate>{rfc2822_now()}</lastBuildDate>")

    for it in items:
        parts.append("<item>")
        parts.append(f"<title><![CDATA[{it['title']}]]></title>")
        parts.append(f"<link>{it['link']}</link>")
        parts.append(f"<guid isPermaLink='false'>{it['guid']}</guid>")
        parts.append(f"<pubDate>{it['pubDate']}</pubDate>")
        parts.append(f"<description><![CDATA[{it['description']}]]></description>")
        parts.append("</item>")

    parts.append("</channel>")
    parts.append("</rss>")

    RSS_PATH.write_text("\n".join(parts) + "\n", encoding="utf-8")


def main():
    state = safe_load_state()
    html = fetch_html()
    last_update_text = extract_last_update(html)
    stations = parse_rios_table(html)
    items = build_items(state, stations, last_update_text)
    write_rss(items)
    save_state(state)


if __name__ == "__main__":
    main()
