#!/usr/bin/env python3
"""
sync_sheets.py
Busca o CSV via Google Sheets API autenticando com Service Account,
aplica o mesmo parsing do dashboard e salva como data.json.
"""

import csv
import json
import math
import os
import re
import sys
from datetime import datetime, timezone
from io import StringIO

import requests

SPREADSHEET_ID = "1vSiZc_RmV8VxclJsUUyXhh39_jxF601vx5mXCnkM4GmPHtE3bdc8w15c3kZHHb0i7880Xq2889FIGys"
SHEET_GID      = "0"
OUTPUT_FILE    = "data.json"
SCOPES         = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


def get_access_token(credentials: dict) -> str:
    import base64
    import time
    import urllib.request as _req
    import urllib.parse as _parse

    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding as _pad

        now = int(time.time())
        def b64(d): return base64.urlsafe_b64encode(d).rstrip(b"=")
        header  = b64(json.dumps({"alg":"RS256","typ":"JWT"}).encode())
        payload = b64(json.dumps({
            "iss": credentials["client_email"],
            "scope": " ".join(SCOPES),
            "aud": credentials["token_uri"],
            "iat": now, "exp": now + 3600,
        }).encode())
        msg = header + b"." + payload
        pk  = serialization.load_pem_private_key(credentials["private_key"].encode(), password=None)
        sig = b64(pk.sign(msg, _pad.PKCS1v15(), hashes.SHA256()))
        jwt = (msg + b"." + sig).decode()

    except ImportError:
        from google.oauth2 import service_account
        import google.auth.transport.requests
        creds = service_account.Credentials.from_service_account_info(credentials, scopes=SCOPES)
        creds.refresh(google.auth.transport.requests.Request())
        return creds.token

    data = _parse.urlencode({"grant_type":"urn:ietf:params:oauth:grant-type:jwt-bearer","assertion":jwt}).encode()
    resp = _req.urlopen(_req.Request(credentials["token_uri"], data=data))
    return json.loads(resp.read())["access_token"]


def fetch_csv(credentials: dict) -> str:
    token = get_access_token(credentials)
    url = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/export?format=csv&gid={SHEET_GID}"
    resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=30)
    resp.raise_for_status()
    resp.encoding = "utf-8"
    return resp.text


def parse_number(v):
    if not v: return None
    s = re.sub(r"R\$\s*", "", str(v)).strip()
    if not s or s == "-": return None
    if "," in s and ("." in s or re.search(r",\d{2}$", s)):
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", "")
    try:
        f = float(s); return None if math.isnan(f) else f
    except ValueError: return None

def parse_pct(v):
    if not v: return None
    s0 = str(v).strip(); has_pct = "%" in s0
    s = s0.replace("%", "").strip()
    if "," in s: s = s.replace(".", "").replace(",", ".")
    try: f = float(s)
    except ValueError: return None
    if math.isnan(f): return None
    return f / 100 if (has_pct or abs(f) > 1) else f

def parse_text(v):
    s = str(v or "").strip()
    return s if s and s != "-" and s != "0" else None

def parse_date(v):
    if not v: return None
    s = str(v).strip()
    if not s or s == "-": return None
    if "/" in s:
        p = s.split("/")
        if len(p) == 3:
            dd, mm, yyyy = p
            return f"{yyyy.zfill(4)}-{mm.zfill(2)}-{dd.zfill(2)}"
    if re.match(r"^\d{4}-\d{2}-\d{2}", s): return s[:10]
    try:
        serial = float(s)
        if serial > 40000:
            return datetime.fromtimestamp((serial-25569)*86400, tz=timezone.utc).strftime("%Y-%m-%d")
    except ValueError: pass
    return None

def csv_to_raw(text: str) -> list:
    rows = list(csv.reader(StringIO(text)))
    if len(rows) < 2: return []
    result = []
    for row in rows[1:]:
        c = row
        if not c or not (c[0] if len(c)>0 else "") or not (c[4] if len(c)>4 else ""): continue
        num_str = re.sub(r"\D", "", str(c[0]))
        if not num_str: continue
        num = int(num_str)
        if num == 0: continue
        def col(i): return c[i] if i < len(c) else ""
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
            "confessional": 1 if str(col(32)).strip().upper()=="TRUE" else 0,
            "unidades":     int(col(6)) if str(col(6)).isdigit() else 1,
        })
    return result


def main():
    creds_raw = os.environ.get("GOOGLE_CREDENTIALS")
    if not creds_raw:
        print("[sync] ERRO: GOOGLE_CREDENTIALS não encontrada.", file=sys.stderr); sys.exit(1)
    try:
        credentials = json.loads(creds_raw)
    except json.JSONDecodeError as e:
        print(f"[sync] ERRO ao parsear credenciais: {e}", file=sys.stderr); sys.exit(1)

    print(f"[sync] Autenticando como {credentials.get('client_email','?')}")
    try:
        text = fetch_csv(credentials)
    except Exception as e:
        print(f"[sync] ERRO ao buscar CSV: {e}", file=sys.stderr); sys.exit(1)

    data = csv_to_raw(text)
    if not data:
        print("[sync] ERRO: nenhum registro encontrado.", file=sys.stderr); sys.exit(1)

    now_utc = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump({"updated_at": now_utc, "records": data}, f, ensure_ascii=False, separators=(",",":"))
    print(f"[sync] OK — {len(data)} contratos → {OUTPUT_FILE}  ({now_utc})")

if __name__ == "__main__":
    main()
