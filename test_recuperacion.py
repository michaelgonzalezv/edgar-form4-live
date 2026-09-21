# -*- coding: utf-8 -*-
"""
test_recuperacion.py -- prueba local (remoto git bare + clones, sin red ni SEC) de la recuperacion del detector:
  1. sincronizar_con_origin deja un checkout desactualizado EXACTAMENTE en origin/main.
  2. El modo de falla original (checkout viejo + cambios conflictivos en data/latido.json) reproducido con el watch.py
     ANTERIOR: los push fallan para siempre y el proceso sigue "vivo".
  3. Con el watch.py nuevo: tras 3 commit_y_push fallidos seguidos el proceso termina (SystemExit 3), sin rebase a medias.
  4. Recuperacion de punta a punta: una corrida se desincroniza a mitad, termina; la siguiente arranca sincronizada y
     publica la senal UNA sola vez (sin duplicados); latido.json lleva run_id/sha_inicio.
Uso: python test_recuperacion.py   (desde la raiz del repo edgar-form4-live con el watch.py nuevo)
"""
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import types

AQUI = os.path.dirname(os.path.abspath(__file__))
NUEVO = open(os.path.join(AQUI, "watch.py"), encoding="utf-8").read()
VIEJO = subprocess.run(["git", "-C", AQUI, "show", "HEAD:watch.py"], capture_output=True, text=True).stdout
assert "sincronizar_con_origin" in NUEVO and "sincronizar_con_origin" not in VIEJO, "correr con cambios sin commitear sobre el HEAD anterior"
time.sleep_real = time.sleep


def sh(cwd, *args, check=True):
    return subprocess.run(["git", "-C", cwd, "-c", "user.name=t", "-c", "user.email=t@t", *args], capture_output=True, text=True, check=check)


def montar(codigo):
    """remoto bare + clon 'runner' (con el watch.py dado) + clon 'otro' que avanza main."""
    raiz = tempfile.mkdtemp()
    remoto, runner, otro = (os.path.join(raiz, n) for n in ("remoto.git", "runner", "otro"))
    subprocess.run(["git", "init", "-q", "--bare", "-b", "main", remoto], check=True)
    subprocess.run(["git", "clone", "-q", remoto, otro], check=True, capture_output=True)
    os.makedirs(os.path.join(otro, "data", "inbox"))
    json.dump([], open(os.path.join(otro, "data", "universo.json"), "w"))
    json.dump([], open(os.path.join(otro, "data", "vistos.json"), "w"))
    json.dump({"ultimo_latido": "x"}, open(os.path.join(otro, "data", "latido.json"), "w"))
    open(os.path.join(otro, "data", "inbox", ".gitkeep"), "w").write("")
    open(os.path.join(otro, "watch.py"), "w", encoding="utf-8").write(codigo)
    sh(otro, "add", "-A"); sh(otro, "commit", "-qm", "base"); sh(otro, "push", "-q", "origin", "HEAD:main")
    subprocess.run(["git", "clone", "-q", remoto, runner], check=True, capture_output=True)
    return raiz, remoto, runner, otro


def cargar(runner, nombre):
    stubs = {}
    for m in ("edgar", "pandas"):
        try:
            __import__(m)
        except ImportError:
            stubs[m] = types.SimpleNamespace(set_identity=lambda *a, **k: None, Company=object)
    sys.modules.update(stubs)
    spec = importlib.util.spec_from_file_location(nombre, os.path.join(runner, "watch.py"))
    w = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(w)
    w.time.sleep = lambda s: None            # los reintentos de 2 s y el poll no esperan de verdad
    return w


def avanzar_remoto(otro, texto, vistos=False):
    """otro clon publica un cambio CONFLICTIVO sobre latido.json (y vistos.json) como los de la corrida anterior."""
    json.dump({"ultimo_latido": texto}, open(os.path.join(otro, "data", "latido.json"), "w"))
    if vistos:
        json.dump(["conflicto-" + texto], open(os.path.join(otro, "data", "vistos.json"), "w"))
    sh(otro, "add", "-A"); sh(otro, "commit", "-qm", "avance " + texto); sh(otro, "push", "-q", "origin", "HEAD:main")


def head(d, ref="HEAD"):
    return sh(d, "rev-parse", ref).stdout.strip()


print("=== 1. sincronizar_con_origin ===")
raiz, remoto, runner, otro = montar(NUEVO)
avanzar_remoto(otro, "A")
w = cargar(runner, "w1")
assert head(runner) != head(remoto)
assert w.sincronizar_con_origin() is True and head(runner) == head(remoto, "main")
json.dump({"ultimo_latido": "sucio local"}, open(os.path.join(runner, "data", "latido.json"), "w"))
sh(runner, "add", "-A"); sh(runner, "commit", "-qm", "local sin publicar")
assert w.sincronizar_con_origin() is True and head(runner) == head(remoto, "main"), "descarta commits locales sin publicar"
print("OK  un checkout desactualizado o con commits locales queda EXACTAMENTE en origin/main")

print("\n=== 2. reproduccion del defecto con el watch.py ANTERIOR ===")
raiz, remoto, runner_v, otro = montar(VIEJO)
w_viejo = cargar(runner_v, "wviejo")
avanzar_remoto(otro, "B")                                     # main avanza; el runner se queda en el SHA viejo
antes = head(remoto, "main")
for i in range(4):
    json.dump({"ultimo_latido": f"local {i}"}, open(os.path.join(runner_v, "data", "latido.json"), "w"))
    w_viejo.commit_y_push(f"latido {i}")                      # NO termina: retorna tras 5 reintentos
assert head(remoto, "main") == antes, "ningun push llego a origin: el detector detecta pero no publica"
print("OK  con el codigo anterior: 4 latidos seguidos, 0 publicados, el proceso sigue 'vivo' (el patron de 340 min mudos)")

print("\n=== 3. codigo nuevo: tras 3 fallos consecutivos termina (SystemExit 3) y no deja un rebase a medias ===")
raiz, remoto, runner, otro = montar(NUEVO)
w = cargar(runner, "w3")
avanzar_remoto(otro, "C")
antes = head(remoto, "main")
codigos = []
for i in range(3):
    json.dump({"ultimo_latido": f"local {i}"}, open(os.path.join(runner, "data", "latido.json"), "w"))
    try:
        w.commit_y_push(f"latido {i}")
        codigos.append(None)
    except SystemExit as e:
        codigos.append(e.code)
assert codigos == [None, None, 3], codigos
assert not os.path.exists(os.path.join(runner, ".git", "rebase-merge")) and not os.path.exists(os.path.join(runner, ".git", "rebase-apply")), "sin rebase a medias"
assert head(remoto, "main") == antes
# un push exitoso reinicia el contador
raiz, remoto, runner, otro = montar(NUEVO)
w = cargar(runner, "w3b")
w._fallos_push_consecutivos = 2
json.dump({"ultimo_latido": "ok"}, open(os.path.join(runner, "data", "latido.json"), "w"))
w.commit_y_push("latido ok")
assert w._fallos_push_consecutivos == 0 and head(remoto, "main") == head(runner)
print("OK  fallos 1 y 2: sigue; fallo 3: SystemExit(3); sin rebase abierto; un push exitoso reinicia el contador")

print("\n=== 4. recuperacion de punta a punta sin duplicar senales ===")
raiz, remoto, runner, otro = montar(NUEVO)
ENTRADA = {"cik": 999, "accession_no": "0001-26-000001", "rol": "Issuer", "aceptado_en": "2026-09-21T14:00:00+00:00"}


def preparar(w, sabotear_en_ciclo=None, otro_clon=None):
    w.INTERVALO_POLL = 0
    w.CICLOS_POR_LATIDO = 1
    w.DURACION_MAX = 2.0
    w.SLEEP_ENTRE_FILINGS = 0
    w.requests.get = lambda *a, **k: types.SimpleNamespace(text="", raise_for_status=lambda: None)
    estado = {"ciclo": 0}

    def feed(_txt):
        estado["ciclo"] += 1
        if sabotear_en_ciclo and estado["ciclo"] == sabotear_en_ciclo:
            avanzar_remoto(otro_clon, "desincroniza a mitad de la corrida", vistos=True)     # origin avanza detras del runner
        return [ENTRADA]
    w.parsear_entradas_feed = feed
    w.parsear_form4_puntual = lambda cik, acc, *r: ([{"accession_no": acc, "cik": cik, "ticker": None, "aceptado_en": ENTRADA["aceptado_en"]}], True)


# corrida 1: detecta la senal en el ciclo 1, pero justo antes origin avanza con un cambio conflictivo -> push roto sostenido
w1 = cargar(runner, "run1")
os.environ["GITHUB_RUN_ID"] = "1001"
preparar(w1, sabotear_en_ciclo=1, otro_clon=otro)
try:
    w1.main()
    codigo = None
except SystemExit as e:
    codigo = e.code
assert codigo == 3, f"la corrida con push roto debia terminar con 3, salio {codigo}"
inbox_remoto = subprocess.run(["git", "-C", remoto, "ls-tree", "-r", "--name-only", "main", "data/inbox"], capture_output=True, text=True).stdout.split()
assert [f for f in inbox_remoto if f.endswith(".jsonl")] == [], "la corrida rota NO publico la senal"
# corrida 2 (la que programa el cron): arranca desde el checkout roto de la corrida 1, sincroniza y publica
w2 = cargar(runner, "run2")
os.environ["GITHUB_RUN_ID"] = "1002"
preparar(w2)
w2.main()
sh(otro, "pull", "-q", "--rebase", "origin", "main")
jsonl = [f for f in os.listdir(os.path.join(otro, "data", "inbox")) if f.endswith(".jsonl")]
assert len(jsonl) == 1, f"la senal se publica UNA sola vez, hay {len(jsonl)} archivos"
filas = [json.loads(l) for l in open(os.path.join(otro, "data", "inbox", jsonl[0])) if l.strip()]
assert [f["accession_no"] for f in filas] == [ENTRADA["accession_no"]]
vistos = json.load(open(os.path.join(otro, "data", "vistos.json")))
assert vistos.count(ENTRADA["accession_no"]) == 1
lat = json.load(open(os.path.join(otro, "data", "latido.json")))
assert lat["run_id"] == "1002" and lat["sincronizado_al_inicio"] is True and lat["sha_inicio"] and lat["cerrado_por_duracion_max"] is True, lat
print("OK  corrida 1 termina (3) sin publicar; corrida 2 sincroniza, publica la senal 1 vez, vistos sin duplicados; latido con run_id/sha_inicio")
print("\nTODAS LAS PRUEBAS DE RECUPERACION DEL DETECTOR PASARON")
