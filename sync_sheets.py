#!/usr/bin/env python3
"""
sync_sheets.py — usa google-auth + Google Sheets API v4
"""

import csv, json, math, os, re, sys
from datetime import datetime, timezone
from io import StringIO

SPREADSHEET_ID = "1_Q-Sf6qhQNoLKLqvkcIYpSKYN9H211ehSRt9Wq_GwQ8"
SHEET_GID      = "0"
OUTPUT_FILE    = "data.json"
SCOPES         = ["https://www.googleapis.com/auth/spreadsheets.readonly",
                  "https://www.googleapis.com/auth/drive.readonly"]

def get_session(credentials: dict):
    from google.oauth2 import service_account
    import google.auth.transport.requests
    creds = service_account.Credentials.from_service_account_info(credentials, scopes=SCOPES)
    creds.refresh(google.auth.transport.requests.Request())
    return creds

def fetch_csv(credentials: dict) -> str:
    import requests as req
    creds = get_session(credentials)
    # Sheets API v4 — exporta como CSV
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{SPREADSHEET_ID}/values/A:AZ"
    resp = req.get(url, headers={"Authorization": f"Bearer {creds.token}"}, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    rows = data.get("values", [])
    # Converte para CSV string para reaproveitar o parser existente
    out = []
    for row in rows:
        out.append(",".join('"'+str(c).replace('"','""')+'"' for c in row))
    return "\n".join(out)

def parse_number(v):
    if not v: return None
    s = re.sub(r"R\$\s*","",str(v)).strip()
    if not s or s=="-": return None
    if "," in s and ("." in s or re.search(r",\d{2}$",s)):
        s=s.replace(".","").replace(",",".")
    else:
        s=s.replace(",","")
    try:
        f=float(s); return None if math.isnan(f) else f
    except ValueError: return None

def parse_pct(v):
    if not v: return None
    s0=str(v).strip(); has_pct="%"in s0
    s=s0.replace("%","").strip()
    if ","in s: s=s.replace(".","").replace(",",".")
    try: f=float(s)
    except ValueError: return None
    if math.isnan(f): return None
    return f/100 if(has_pct or abs(f)>1)else f

def parse_text(v):
    s=str(v or"").strip()
    return s if s and s!="-" and s!="0" else None

def parse_date(v):
    if not v: return None
    s=str(v).strip()
    if not s or s=="-": return None
    if "/"in s:
        p=s.split("/")
        if len(p)==3:
            dd,mm,yyyy=p
            return f"{yyyy.zfill(4)}-{mm.zfill(2)}-{dd.zfill(2)}"
    if re.match(r"^\d{4}-\d{2}-\d{2}",s): return s[:10]
    try:
        serial=float(s)
        if serial>40000:
            return datetime.fromtimestamp((serial-25569)*86400,tz=timezone.utc).strftime("%Y-%m-%d")
    except ValueError: pass
    return None

def csv_to_raw(text:str)->list:
    rows=list(csv.reader(StringIO(text)))
    if len(rows)<2: return []
    result=[]
    for row in rows[1:]:
        c=row
        if not c or not(c[0]if len(c)>0 else"")or not(c[4]if len(c)>4 else""): continue
        num_str=re.sub(r"\D","",str(c[0]))
        if not num_str: continue
        num=int(num_str)
        if num==0: continue
        def col(i): return c[i]if i<len(c)else""
        result.append({
            "num":num,
            "executivo":   parse_text(col(1)),
            "lideranca":   parse_text(col(2)),
            "escola":      parse_text(col(4)),
            "fat_escola":  parse_number(col(7)),
            "fat_kedu":    parse_number(col(8)),
            "taxa":        parse_pct(col(9)),
            "alunos":      parse_number(col(10)),
            "ticket_medio":parse_number(col(11)),
            "spread":      parse_pct(col(13)),
            "margem":      parse_pct(col(14)),
            "modelo":      parse_text(col(15)),
            "status":      parse_text(col(17)),
            "cidade":      parse_text(col(19)),
            "estado":      parse_text(col(20)),
            "assinatura":  parse_date(col(24)),
            "confessional":1 if str(col(32)).strip().upper()=="TRUE" else 0,
            "unidades":    int(col(6))if str(col(6)).isdigit()else 1,
            "garantido":   parse_number(col(33)),
            "margem_rs":   parse_number(col(34)),
        })
    return result

def main():
    creds_raw=os.environ.get("GOOGLE_CREDENTIALS")
    if not creds_raw:
        print("[sync] ERRO: GOOGLE_CREDENTIALS não encontrada.",file=sys.stderr); sys.exit(1)
    try:
        credentials=json.loads(creds_raw)
    except json.JSONDecodeError as e:
        print(f"[sync] ERRO ao parsear credenciais: {e}",file=sys.stderr); sys.exit(1)

    print(f"[sync] Autenticando como {credentials.get('client_email','?')}")
    try:
        text=fetch_csv(credentials)
    except Exception as e:
        print(f"[sync] ERRO ao buscar dados: {e}",file=sys.stderr); sys.exit(1)

    data=csv_to_raw(text)
    if not data:
        print("[sync] ERRO: nenhum registro encontrado.",file=sys.stderr); sys.exit(1)

    now_utc=datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with open(OUTPUT_FILE,"w",encoding="utf-8") as f:
        json.dump({"updated_at":now_utc,"records":data},f,ensure_ascii=False,separators=(",",":"))
    print(f"[sync] OK — {len(data)} contratos → {OUTPUT_FILE}  ({now_utc})")

if __name__=="__main__":
    main()
