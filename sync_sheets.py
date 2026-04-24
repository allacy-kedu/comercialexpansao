#!/usr/bin/env python3
"""
sync_sheets.py
Busca o CSV público do Google Sheets, aplica o mesmo parsing
do dashboard e salva como data.json na raiz do repositório.
"""

import csv
import json
import math
import re
import sys
from datetime import datetime, timezone
from io import StringIO

import requests

GS_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vSiZc_RmV8VxclJsUUyXhh39_jxF601vx5mXCnkM4GmPHtE3bdc8w15c3kZHHb0i7880Xq2889FIGys"
    "/pub?gid=0&single=true&output=csv"
)

OUTPUT_FILE = "data.json"


# ── Helpers de parsing (espelham o JS do dashboard) ────────────

def parse_number(v):
    """R$ 2.360.000,00 | 2360000 | 2360000.5  →  float | None"""
    if not v:
        return None
    s = re.sub(r"R\$\s*", "", str(v)).strip()
    if not s or s == "-":
        return None
    # formato BR: ponto como milhar, vírgula como decimal
    if "," in s and ("." in s or re.search(r",\d{2}$", s)):
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", "")
    try:
        f = float(s)
        return None if math.isnan(f) else f
    except ValueError:
        return None


def parse_pct(v):
    """'10,00%' → 0.10  |  '0.1' → 0.1  |  None"""
    if not v:
        return None
    s0 = str(v).strip()
    has_pct = "%" in s0
    s = s0.replace("%", "").strip()
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        f = float(s)
    except ValueError:
        return None
    if math.isnan(f):
        return None
    return f / 100 if (has_pct or abs(f) > 1) else f


def parse_text(v):
    s = str(v or "").strip()
    return s if s and s != "-" and s != "0" else None


def parse_date(v):
    """DD/MM/YYYY | YYYY-MM-DD | serial GSheets  →  'YYYY-MM-DD' | None"""
    if not v:
        return None
    s = str(v).strip()
    if not s or s == "-":
        return None
    if "/" in s:
        parts = s.split("/")
        if len(parts) == 3:
            dd, mm, yyyy = parts
            return f"{yyyy.zfill(4)}-{mm.zfill(2)}-{dd.zfill(2)}"
    if re.match(r"^\d{4}-\d{2}-\d{2}", s):
        return s[:10]
    # serial GSheets (dias desde 1899-12-30)
    try:
        serial = float(s)
        if serial > 40000:
            ts = (serial - 25569) * 86400
            return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
    except ValueError:
        pass
    return None


# ── Fetch + parse ───────────────────────────────────────────────

def fetch_csv(url: str) -> str:
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    resp.encoding = "utf-8"
    return resp.text


def csv_to_raw(text: str) -> list[dict]:
    reader = csv.reader(StringIO(text))
    rows = list(reader)
    if len(rows) < 2:
        return []

    result = []
    for row in rows[1:]:          # pula cabeçalho
        c = row
        if not c or not (c[0] if len(c) > 0 else "") or not (c[4] if len(c) > 4 else ""):
            continue

        # Nº — pode ser "1ª" ou "1"
        num_str = re.sub(r"\D", "", str(c[0]))
        if not num_str:
            continue
        num = int(num_str)
        if num == 0:
            continue

        def col(i):
            return c[i] if i < len(c) else ""

        confessional_raw = str(col(32)).strip().upper()

        result.append({
            "num":          num,
            "executivo":    parse_text(col(1)),
            "lideranca":    parse_text(col(2)),
            "escola":       parse_text(col(4)),
            "fat_escola":   parse_number(col(7)),
            "fat_kedu":     parse_number(col(8)),
            "taxa":         parse_pct(col(9)),
            "alunos":       parse_number(col(10)),
            "ticket_medio": parse_number(col(11)),
            "spread":       parse_pct(col(13)),
            "margem":       parse_pct(col(14)),
            "modelo":       parse_text(col(15)),
            "status":       parse_text(col(17)),
            "cidade":       parse_text(col(19)),
            "estado":       parse_text(col(20)),
            "assinatura":   parse_date(col(24)),
            "confessional": 1 if confessional_raw == "TRUE" else 0,
            "unidades":     int(col(6)) if col(6).isdigit() else 1,
        })

    return result


# ── Main ────────────────────────────────────────────────────────

def main():
    print(f"[sync] Buscando CSV em: {GS_URL}")
    try:
        text = fetch_csv(GS_URL)
    except Exception as e:
        print(f"[sync] ERRO ao buscar CSV: {e}", file=sys.stderr)
        sys.exit(1)

    data = csv_to_raw(text)
    if not data:
        print("[sync] ERRO: nenhum registro encontrado no CSV.", file=sys.stderr)
        sys.exit(1)

    now_utc = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    output = {
        "updated_at": now_utc,
        "records":    data,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, separators=(",", ":"))

    print(f"[sync] OK — {len(data)} contratos → {OUTPUT_FILE}  ({now_utc})")


if __name__ == "__main__":
    main()
