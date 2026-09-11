"""
watch.py -- feed CASI EN TIEMPO REAL de Form 4 (SEC EDGAR), 2026-09-11.

POR QUE ESTO Y NO edgar-form4-feed (el otro repo, cada 30 min rotando
empresas). Esa arquitectura tiene un piso de latencia de ~1.3h por
empresa -- inaceptable para operar "al minuto de leerlo". El cambio de
fondo no es "revisar mas seguido", es dejar de revisar EMPRESA POR EMPRESA
y escuchar el feed UNICO de EDGAR de ultimos filings:

    https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4...

Este feed lista los Form 4 mas recientes de TODA la SEC, un solo request,
actualizado a los pocos segundos de que SEC acepta el filing (medido
2026-09-11: entradas con timestamp <updated> de 15-30s antes del momento
de la consulta). No existe nada mas rapido y publico que esto -- es la
puerta de entrada misma de EDGAR, no hay "proceso interno" adicional
despues de la aceptacion.

POR QUE UN LOOP CONTINUO Y NO CRON CADA 5 MIN. GitHub Actions no deja
programar `schedule` mas seguido que cada 5 minutos (limite de la
plataforma). Para bajar de ahi a "segundos" hace falta un job que NO se
reinicie por cron sino que loopee internamente -- por eso este script
corre su propio `while` por horas en vez de chequear una vez y salir.

POR QUE ESTE REPO ES PUBLICO. Un loop continuo consume minutos de Actions
sin parar (~5h40min por corrida, reiniciandose solo). Eso es gratis
ILIMITADO en un repo publico; en uno privado agotaria el presupuesto
gratis de 2,000 min/mes en menos de dos dias. El dato es 100% publico
(lo mismo que ya publica la SEC) -- no hay nada propietario expuesto aca,
la logica de trading vive en Information Factor (privado, local).

QUE HACE.
  1. Cada INTERVALO_POLL segundos, pide el feed de ultimos Form 4.
  2. Cruza cada entrada contra data/universo.json (las 788 empresas de
     Information Factor) por CIK.
  3. Si el accession_no no esta en data/vistos.json, lo parsea completo
     (Company(ticker).get_filings() filtrado a ESE accession_no puntual --
     no escanea el historial, se detiene apenas lo encuentra) y lo agrega
     al buffer.
  4. Apenas hay algo nuevo en el buffer, lo escribe a
     data/inbox/<timestamp>.jsonl, commitea y pushea DE INMEDIATO -- no se
     bufferea por tiempo, la prioridad es que aparezca en git lo antes
     posible.
  5. Se detiene solo un rato antes del limite duro de 6h de los runners
     hosted de GitHub (DURACION_MAX), para que el siguiente disparo de
     cron (cada 5 min, con concurrency-group) tome la posta sin hueco.

Mezcla a edgar_data.db: mismo mecanismo que edgar-form4-feed, ver README.
"""
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone

import requests
import pandas as pd
from edgar import set_identity, Company

set_identity("Michael michael.gonzalez@correounivalle.edu.co")

BASE = os.path.dirname(os.path.abspath(__file__))
UNIVERSO_PATH = os.path.join(BASE, "data", "universo.json")
VISTOS_PATH = os.path.join(BASE, "data", "vistos.json")
LATIDO_PATH = os.path.join(BASE, "data", "latido.json")
INBOX_DIR = os.path.join(BASE, "data", "inbox")

FEED_URL = ("https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent"
            "&type=4&company=&dateb=&owner=include&count=100&output=atom")
HEADERS = {"User-Agent": "Michael michael.gonzalez@correounivalle.edu.co"}

INTERVALO_POLL = 20          # segundos entre consultas al feed
DURACION_MAX = int(os.environ.get("WATCH_DURACION_SEG", 5 * 3600 + 40 * 60))  # override para pruebas cortas
MAX_VISTOS = 20_000           # recorte del set de accession_no ya vistos

RE_CIK = re.compile(r"\((\d{7,10})\)\s*\((?:Filer|Subject|Reporting)\)")
RE_ACCNO = re.compile(r"AccNo:</b>\s*([\d-]+)")
RE_FECHA = re.compile(r"Filed:</b>\s*([\d-]+)")


def cargar_json(path, default):
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return default


def guardar_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)


def git(*args, check=True):
    return subprocess.run(["git", "-C", BASE, *args], check=check,
                           capture_output=True, text=True)


def commit_y_push(mensaje):
    git("add", "data")
    diff = git("diff", "--cached", "--quiet", check=False)
    if diff.returncode == 0:
        return  # nada para commitear
    git("commit", "-m", mensaje)
    for intento in range(5):
        r = git("push", check=False)
        if r.returncode == 0:
            return
        git("pull", "--rebase", check=False)
        time.sleep(2)
    print("AVISO: push fallo tras 5 reintentos, sigue en el proximo commit")


def parsear_entradas_feed(xml_text):
    """Parseo minimo por regex, no XML completo -- el feed atom de EDGAR
    es simple y esto evita una dependencia extra. entry -> (titulo, cik,
    accession_no, filing_date).

    FILTRO EXACTO DE FORM TYPE (encontrado 2026-09-11 probando en vivo):
    el parametro `type=4` de la URL hace match por PREFIJO del lado del
    servidor de SEC, no exacto -- devuelve tambien 424B2, 4/A, etc. Sin
    este filtro, JPMorgan/BofA/Barclays (que filean 424B2 constantemente,
    prospectos de bonos, no Form 4 de insiders) aparecian como "matches"
    del universo y se hubieran procesado como si fueran transacciones de
    insiders. El <category term=...> SI es el form type exacto."""
    entradas = []
    for bloque in xml_text.split("<entry>")[1:]:
        titulo_m = re.search(r"<title>(.*?)</title>", bloque, re.S)
        cik_m = RE_CIK.search(bloque)
        acc_m = RE_ACCNO.search(bloque)
        fecha_m = RE_FECHA.search(bloque)
        tipo_m = re.search(r'label="form type" term="([^"]+)"', bloque)
        if not (titulo_m and cik_m and acc_m and tipo_m):
            continue
        if tipo_m.group(1) not in ("4", "4/A"):
            continue
        entradas.append({
            "titulo": titulo_m.group(1),
            "cik": int(cik_m.group(1)),
            "accession_no": acc_m.group(1),
            "filing_date": fecha_m.group(1) if fecha_m else None,
            "form_type": tipo_m.group(1),
        })
    return entradas


def parsear_form4_puntual(ticker, cik, accession_no):
    """Busca ESE accession_no puntual en los filings recientes de la
    empresa -- no escanea el historial completo. El feed ya nos dijo que
    es nuevo, esto solo trae el detalle (owners, shares, price, code)."""
    try:
        company = Company(ticker)
        filings = company.get_filings(form="4").head(50)
    except Exception as e:
        print(f"    ERROR abriendo {ticker}: {e}")
        return []

    objetivo = None
    for f in filings:
        if f.accession_no == accession_no:
            objetivo = f
            break
    if objetivo is None:
        print(f"    {ticker} {accession_no}: no encontrado en los ultimos 50 (raro, revisar)")
        return []

    try:
        form4 = objetivo.obj()
    except Exception as e:
        print(f"    ERROR parseando {ticker} {accession_no}: {e}")
        return []
    if form4 is None:
        return []

    filing_date = str(objetivo.filing_date)
    form_type = getattr(objetivo, "form", None)
    owners_list = form4.reporting_owners.owners
    owner_names = "; ".join(o.name or "" for o in owners_list)
    owner_titles = "; ".join(o.officer_title or "" for o in owners_list if o.is_officer)
    is_officer_any = any(o.is_officer for o in owners_list)
    is_director_any = any(o.is_director for o in owners_list)
    is_ten_pct_any = any(o.is_ten_pct_owner for o in owners_list)

    nd_table = form4.non_derivative_table
    parts = []
    if form4.market_trades is not None and not form4.market_trades.empty:
        parts.append(form4.market_trades)
    if nd_table is not None and nd_table.non_market_trades is not None and not nd_table.non_market_trades.empty:
        parts.append(nd_table.non_market_trades)
    if not parts:
        return []
    trades = pd.concat(parts, ignore_index=True)
    footnote_map = form4.footnotes or {}

    filas = []
    for _, row in trades.iterrows():
        shares = row.get("Shares")
        if shares is not None and not isinstance(shares, (int, float)):
            try:
                shares = float(shares)
            except (TypeError, ValueError):
                shares = None
        code = row.get("Code")
        acquired_disposed = row.get("AcquiredDisposed")
        signed_shares = shares if acquired_disposed == "A" else (-shares if shares is not None else None)
        raw_fn_ref = row.get("footnotes")
        refs = re.findall(r"F\d+", str(raw_fn_ref)) if raw_fn_ref else []
        footnote_texts = [footnote_map.get(r) for r in refs if footnote_map.get(r)]

        filas.append({
            "cik": cik, "ticker": ticker, "accession_no": accession_no,
            "filing_date": filing_date, "form_type": form_type,
            "owner_names": owner_names, "owner_titles": owner_titles,
            "is_officer": is_officer_any, "is_director": is_director_any,
            "is_ten_pct_owner": is_ten_pct_any,
            "security": row.get("Security"), "transaction_date": str(row.get("Date")),
            "code": code, "shares": shares, "signed_shares": signed_shares,
            "acquired_disposed": acquired_disposed, "price": row.get("Price"),
            "shares_remaining": row.get("Remaining"),
            "direct_indirect": row.get("DirectIndirect"),
            "nature_of_ownership": row.get("NatureOfOwnership"),
            "equity_swap": row.get("EquitySwap"),
            "transaction_type": row.get("TransactionType"),
            "footnote_text": " || ".join(t for t in footnote_texts if t) if footnote_texts else None,
            "detectado_en": datetime.now(timezone.utc).isoformat(),
        })
    return filas


def main():
    os.makedirs(INBOX_DIR, exist_ok=True)
    universo = cargar_json(UNIVERSO_PATH, [])
    por_cik = {int(e["cik"]): e["ticker"] for e in universo}
    print(f"universo: {len(por_cik)} empresas")

    vistos = set(cargar_json(VISTOS_PATH, []))
    inicio = time.time()
    ciclos = 0
    total_encontradas = 0

    while time.time() - inicio < DURACION_MAX:
        ciclos += 1
        try:
            r = requests.get(FEED_URL, headers=HEADERS, timeout=15)
            r.raise_for_status()
            entradas = parsear_entradas_feed(r.text)
        except Exception as e:
            print(f"  ciclo {ciclos}: error consultando el feed: {e}")
            time.sleep(INTERVALO_POLL)
            continue

        nuevas_filas = []
        for e in entradas:
            if e["cik"] not in por_cik:
                continue
            if e["accession_no"] in vistos:
                continue
            vistos.add(e["accession_no"])
            ticker = por_cik[e["cik"]]
            print(f"  ciclo {ciclos}: {ticker} ({e['cik']}) {e['accession_no']} -- parseando...")
            filas = parsear_form4_puntual(ticker, e["cik"], e["accession_no"])
            if filas:
                nuevas_filas.extend(filas)

        if nuevas_filas:
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
            out_path = os.path.join(INBOX_DIR, f"{ts}.jsonl")
            with open(out_path, "w", encoding="utf-8") as f:
                for fila in nuevas_filas:
                    f.write(json.dumps(fila, ensure_ascii=False) + "\n")
            total_encontradas += len(nuevas_filas)
            print(f"  {len(nuevas_filas)} transacciones nuevas -> {out_path}")

            if len(vistos) > MAX_VISTOS:
                vistos = set(list(vistos)[-MAX_VISTOS:])
            guardar_json(VISTOS_PATH, sorted(vistos))
            commit_y_push(f"watch: {len(nuevas_filas)} transacciones nuevas "
                           f"({datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')})")

        if ciclos % 30 == 0:  # latido cada ~10 min (30 ciclos x 20s)
            guardar_json(LATIDO_PATH, {
                "ultimo_latido": datetime.now(timezone.utc).isoformat(),
                "ciclos": ciclos, "total_encontradas": total_encontradas,
            })
            commit_y_push(f"watch: latido ciclo {ciclos}")
            guardar_json(VISTOS_PATH, sorted(vistos))  # persistir vistos igual sin hallazgos

        time.sleep(INTERVALO_POLL)

    print(f"fin del loop: {ciclos} ciclos, {total_encontradas} transacciones en "
          f"{(time.time()-inicio)/60:.1f} min -- el proximo disparo de cron toma la posta")
    guardar_json(VISTOS_PATH, sorted(vistos))
    guardar_json(LATIDO_PATH, {
        "ultimo_latido": datetime.now(timezone.utc).isoformat(),
        "ciclos": ciclos, "total_encontradas": total_encontradas, "cerrado_por_duracion_max": True,
    })
    commit_y_push(f"watch: cierre de corrida ({ciclos} ciclos, {total_encontradas} hallazgos)")


if __name__ == "__main__":
    main()
