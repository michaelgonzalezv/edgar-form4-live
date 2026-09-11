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

# BUG CRITICO ENCONTRADO 2026-09-11, despues de desplegar: este regex
# exigia el rol (Filer|Subject|Reporting) y NO aceptaba (Issuer). En un
# Form 4 la SEC emite una entrada por cada parte:
#     4 - Ladiwala Shiraz Shabanali (0001715573) (Reporting)   <- la PERSONA
#     4 - MESA LABORATORIES INC /CO/ (0000724004) (Issuer)     <- la EMPRESA
# Nuestro universo son CIKs de EMPRESAS, o sea que la unica entrada que
# puede matchear es la de (Issuer) -- justo la que el regex descartaba.
# Con el regex viejo el watcher NO PODIA disparar nunca: matcheaba solo
# entradas (Reporting), cuyo CIK es el de la persona fisica y jamas esta
# en el universo. Los "0 hallazgos" parecian un viernes tranquilo y en
# realidad era el sistema muerto -- el modo de falla mas peligroso de
# todos, porque "no encontro nada" y "esta roto" se ven igual.
# Ahora se acepta cualquier rol y se filtra por CIK: el CIK de un insider
# persona nunca colisiona con el de una empresa del universo, asi que
# matchear "cualquier rol" es seguro y ademas a prueba de que la SEC
# cambie las etiquetas.
RE_CIK = re.compile(r"\((\d{7,10})\)\s*\(([^)]+)\)")
# SEGUNDO BUG CRITICO ENCONTRADO 2026-09-11 (independiente del de RE_CIK,
# cada uno por su cuenta ya dejaba el watcher muerto): estos regex eran
#     r"AccNo:</b>\s*([\d-]+)"   y   r"Filed:</b>\s*([\d-]+)"
# pero el <summary> del feed viene con las etiquetas ESCAPADAS como
# entidades HTML, no como markup literal:
#     &lt;b&gt;Filed:&lt;/b&gt; 2026-09-11 &lt;b&gt;AccNo:&lt;/b&gt; 0000724004-26-000096
# o sea que `</b>` nunca aparecia y el regex NUNCA matcheaba ->
# parsear_entradas_feed() devolvia [] en todos los ciclos, para siempre.
# Se toma el accession del <id> y la fecha/hora del <updated>, que vienen
# sin escapar. Ademas <updated> es MEJOR que "Filed:": trae la hora exacta
# de aceptacion con segundos (justo el dato que la seccion 60 del proyecto
# tuvo que salir a buscar aparte), y permite medir la latencia real de
# deteccion en vez de suponerla.
RE_ACCNO = re.compile(r"accession-number=([\d-]+)")
RE_UPDATED = re.compile(r"<updated>([^<]+)</updated>")


def cargar_json(path, default):
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return default


def guardar_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)


def marcar_visto(vistos, orden_vistos, accession_no):
    """Marca un accession_no como procesado, con purga POR ANTIGUEDAD.

    Antes esto era `vistos = set(list(vistos)[-MAX_VISTOS:])`, que esta mal:
    un `set` de Python no tiene orden, asi que ese recorte tiraba un
    subconjunto arbitrario (el que quedara segun el hash), no los mas
    viejos -- podia descartar un accession recien visto y reprocesarlo. Con
    una lista paralela el orden de insercion es real y la purga saca por la
    punta vieja."""
    if accession_no in vistos:
        return
    vistos.add(accession_no)
    orden_vistos.append(accession_no)
    while len(orden_vistos) > MAX_VISTOS:
        vistos.discard(orden_vistos.pop(0))


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
        upd_m = RE_UPDATED.search(bloque)
        tipo_m = re.search(r'label="form type" term="([^"]+)"', bloque)
        if not (titulo_m and cik_m and acc_m and tipo_m):
            continue
        if tipo_m.group(1) not in ("4", "4/A"):
            continue
        aceptado = upd_m.group(1) if upd_m else None
        entradas.append({
            "titulo": titulo_m.group(1),
            "cik": int(cik_m.group(1)),
            "rol": cik_m.group(2),          # Issuer / Reporting / Filer...
            "accession_no": acc_m.group(1),
            "aceptado_en": aceptado,        # hora exacta de aceptacion en EDGAR
            "filing_date": aceptado[:10] if aceptado else None,
            "form_type": tipo_m.group(1),
        })
    return entradas


def parsear_form4_puntual(ticker, cik, accession_no):
    """Busca ESE accession_no puntual en los filings recientes de la
    empresa -- no escanea el historial completo. El feed ya nos dijo que
    es nuevo, esto solo trae el detalle (owners, shares, price, code).

    Devuelve (filas, resuelto). `resuelto` distingue dos casos que NO son
    lo mismo y que al principio se trataban igual (bug 2026-09-11):
      - resuelto=True  : se llego al documento y se leyo. Puede devolver
                         cero filas legitimamente (un Form 4 solo de
                         derivados, por ejemplo) -- eso NO se reintenta.
      - resuelto=False : no se pudo llegar/parsear (timeout, error de
                         edgartools, todavia no visible en el indice de la
                         empresa). Eso SI se reintenta en el proximo ciclo.
    Sin esta distincion habia que elegir entre perder filings por un error
    transitorio, o reintentar para siempre los que no tienen filas."""
    try:
        company = Company(ticker)
        filings = company.get_filings(form="4").head(50)
    except Exception as e:
        print(f"    ERROR abriendo {ticker}: {e}")
        return [], False

    objetivo = None
    for f in filings:
        if f.accession_no == accession_no:
            objetivo = f
            break
    if objetivo is None:
        # Caso normal, no error: el feed global suele publicar el filing
        # unos segundos antes de que aparezca en el indice por empresa.
        print(f"    {ticker} {accession_no}: todavia no esta en el indice de la empresa")
        return [], False

    try:
        form4 = objetivo.obj()
    except Exception as e:
        print(f"    ERROR parseando {ticker} {accession_no}: {e}")
        return [], False
    if form4 is None:
        return [], True  # pre-2003 sin XML: no hay nada que sacar, no reintentar

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
        return [], True  # Form 4 leido bien pero sin Table I (solo derivados)
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
    return filas, True


def main():
    os.makedirs(INBOX_DIR, exist_ok=True)
    universo = cargar_json(UNIVERSO_PATH, [])
    por_cik = {int(e["cik"]): e["ticker"] for e in universo}
    print(f"universo: {len(por_cik)} empresas")

    # se persiste como LISTA en orden de insercion (no `sorted`), para que
    # la purga por antiguedad de marcar_visto() sobreviva a un reinicio.
    orden_vistos = list(cargar_json(VISTOS_PATH, []))
    vistos = set(orden_vistos)
    inicio = time.time()
    ciclos = 0
    total_encontradas = 0

    while time.time() - inicio < DURACION_MAX:
        ciclos += 1
        entradas = []
        try:
            r = requests.get(FEED_URL, headers=HEADERS, timeout=15)
            r.raise_for_status()
            entradas = parsear_entradas_feed(r.text)
        except Exception as e:
            # NO se hace `continue` aca (bug 2026-09-11): el `continue`
            # saltaba tambien el bloque del latido de mas abajo, asi que
            # una racha de timeouts de SEC -- que ya se vieron, 2 en 4
            # minutos de prueba local -- dejaba latido.json congelado y
            # hacia parecer que el loop estaba muerto cuando seguia vivo.
            print(f"  ciclo {ciclos}: error consultando el feed: {e}")

        nuevas_filas = []
        for e in entradas:
            if e["cik"] not in por_cik:
                continue
            if e["accession_no"] in vistos:
                continue
            ticker = por_cik[e["cik"]]
            print(f"  ciclo {ciclos}: {ticker} ({e['cik']}, {e['rol']}) {e['accession_no']} -- parseando...")
            filas, resuelto = parsear_form4_puntual(ticker, e["cik"], e["accession_no"])
            # MARCAR COMO VISTO SOLO SI SE RESOLVIO (bug 2026-09-11): antes
            # se marcaba ANTES de parsear, asi que un timeout o un error
            # puntual de edgartools quemaba ese accession_no para siempre
            # -- se perdia la senal sin dejar rastro. Ahora un fallo lo
            # deja sin marcar y el proximo ciclo (20s despues) reintenta.
            if resuelto:
                marcar_visto(vistos, orden_vistos, e["accession_no"])
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

            guardar_json(VISTOS_PATH, orden_vistos)
            commit_y_push(f"watch: {len(nuevas_filas)} transacciones nuevas "
                           f"({datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')})")

        if ciclos % 30 == 0:  # latido cada ~10 min (30 ciclos x 20s)
            guardar_json(LATIDO_PATH, {
                "ultimo_latido": datetime.now(timezone.utc).isoformat(),
                "ciclos": ciclos, "total_encontradas": total_encontradas,
            })
            commit_y_push(f"watch: latido ciclo {ciclos}")
            guardar_json(VISTOS_PATH, orden_vistos)  # persistir vistos igual sin hallazgos

        time.sleep(INTERVALO_POLL)

    print(f"fin del loop: {ciclos} ciclos, {total_encontradas} transacciones en "
          f"{(time.time()-inicio)/60:.1f} min -- el proximo disparo de cron toma la posta")
    guardar_json(VISTOS_PATH, orden_vistos)
    guardar_json(LATIDO_PATH, {
        "ultimo_latido": datetime.now(timezone.utc).isoformat(),
        "ciclos": ciclos, "total_encontradas": total_encontradas, "cerrado_por_duracion_max": True,
    })
    commit_y_push(f"watch: cierre de corrida ({ciclos} ciclos, {total_encontradas} hallazgos)")


if __name__ == "__main__":
    main()
