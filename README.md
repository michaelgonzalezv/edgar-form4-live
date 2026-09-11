# edgar-form4-live

Feed de Form 4 (SEC EDGAR) en **casi tiempo real** para Information Factor.
Detecta un filing nuevo a los 20-40 segundos de que la SEC lo acepta (no
"cada 30 minutos" como el hermano `edgar-form4-feed`).

## Por que existe, y por que es distinto de edgar-form4-feed

`edgar-form4-feed` (en `systematic-trading-research`, privado) rota 18 de
788 empresas cada 30 min -- el universo completo tarda ~1.3h en darse una
vuelta. Sirve para mantener la base al dia, pero es inutil si la idea es
"leer la senal y decidir en el minuto".

Este repo hace algo distinto: en vez de preguntarle a cada empresa "tenes
algo nuevo?", **escucha el feed unico de EDGAR de ultimos filings**
(`action=getcurrent&type=4`) -- un solo request que lista los Form 4 mas
recientes de TODA la SEC, medido actualizandose a los 15-30 segundos de
que ocurre la aceptacion. No hay nada mas rapido publico que esto.

## Por que un loop continuo, y por que este repo es publico

GitHub Actions no deja programar `schedule` mas seguido que cada 5
minutos -- para bajar de ahi a "segundos" hace falta un job que loopee
internamente en vez de reiniciarse por cron (`watch.py` hace su propio
`while` por ~5h40min y se cierra solo antes del limite duro de 6h de los
runners hosted; el cron cada 5 min con `concurrency` solo asegura que el
siguiente arranca sin hueco).

Un loop de ~5h40min corriendo case sin parar consume MUCHOS minutos de
Actions -- gratis e ilimitado en un repo PUBLICO, agotaria el presupuesto
gratis de un repo privado (2,000 min/mes) en menos de dos dias. El dato
acá es 100% publico (lo mismo que ya publica la SEC), por eso este repo es
publico y `edgar-form4-feed` (que sí toca cosas de investigacion) sigue
privado.

## Que hace

1. Cada 20s, pide el feed de ultimos Form 4 (`watch.py`).
2. Cruza cada entrada contra `data/universo.json` (las 788 empresas de
   Information Factor) por CIK.
3. Si es nuevo (`data/vistos.json` no lo tiene), busca ESE accession_no
   puntual en los ultimos filings de la empresa (no escanea el historial)
   y extrae owners/shares/price/code.
4. Apenas hay algo, lo escribe en `data/inbox/<timestamp>.jsonl`,
   commitea y pushea DE INMEDIATO -- prioridad en que aparezca en git lo
   antes posible, no se bufferea por tiempo.
5. `data/latido.json` se actualiza cada ~10 min aunque no haya hallazgos,
   para poder confirmar que el loop sigue vivo sin esperar un filing real.

## Mezcla a edgar_data.db

Mismo mecanismo y mismo script que `edgar-form4-feed`:
`63_mezclar_feed_github.py` en Information Factor ya revisa los dos repos.

## Correr manual / ver si esta vivo

Pestana Actions -> "Watch Form 4 (casi tiempo real)" -> "Run workflow".
Para confirmar que esta corriendo: `data/latido.json` en este repo,
`ultimo_latido` no deberia tener mas de ~10-15 min de atraso salvo que
justo este en el hueco de reinicio (segundos, no minutos).
