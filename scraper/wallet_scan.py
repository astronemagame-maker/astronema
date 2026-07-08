"""
Wallet registry — deep CollectChain scan + daily maintenance.

But (finalites 5/6 de Preda) : dater la 1ere apparition on-chain de CHAQUE
wallet ("anciennete") pour detecter les wallets reellement nouveaux chaque
jour, sans jamais mettre ~700k lignes dans le Google Sheet.

Deux CSV sous data/ (commites dans le repo par les workflows) :

    wallet_registry_deep.csv   ecrit UNIQUEMENT par le workflow wallet-scan.yml
                               (scan du present vers la genese, en tranches
                               resumables ; fige une fois le scan termine).
    wallet_registry_daily.csv  ecrit UNIQUEMENT par le run chain quotidien
                               (scraper.chain_run -> update_from_records) ;
                               couvre tout depuis le lancement du scan profond.

Les consommateurs FUSIONNENT les deux fichiers (min first_seen / max
last_active / somme tx_count). Aucun workflow n'ecrit le fichier de l'autre
-> aucun conflit git possible.

Dates en PT (America/Los_Angeles), le fuseau metier de VeVe.
first_seen = date du 1er transfert du wallet (recu ou envoye) sur CollectChain.
Les wallets anterieurs a la migration IMX->CollectChain afficheront la date de
migration (choix acte : CollectChain seul).

Etat du scan (data/wallet_scan_state.json) :
    next_page_params  curseur keyset pour reprendre le scan
    pages/transfers   compteurs cumules
    oldest_ts         jusqu'ou on est remonte
    done              true une fois la genese atteinte
    runs              nombre de tranches executees

Env (scan profond) :
    SCAN_MINUTES    budget temps par run (defaut 280 — tient dans un job 6 h)
    SCAN_MAX_PAGES  budget pages par run (defaut 0 = illimite)
    SCAN_PAUSE      pause entre pages (defaut 0.05 s ; augmenter = discretion)
"""

from __future__ import annotations

import csv
import datetime as _dt
import json
import os
import sys
import time
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from scraper import collectchain as cc

DATA_DIR = os.environ.get("WALLET_DATA_DIR", "data")
DEEP_CSV = os.path.join(DATA_DIR, "wallet_registry_deep.csv")
DAILY_CSV = os.path.join(DATA_DIR, "wallet_registry_daily.csv")
STATE_JSON = os.path.join(DATA_DIR, "wallet_scan_state.json")

PT = ZoneInfo("America/Los_Angeles")
HEADER = ["wallet", "first_seen", "last_active", "tx_count"]
SAVE_EVERY_PAGES = 2000          # checkpoint intermediaire (crash-safety)
COUNTERS_URL = f"{cc.API_BASE}/tokens/{cc.CONTRACT}/counters"

# Adresses systeme exclues du registre.
_SKIP = {cc.ZERO, cc.MARKET_ESCROW, ""}


def _pt_date(ts: _dt.datetime) -> str:
    """Naive-UTC datetime -> date PT (YYYY-MM-DD)."""
    return ts.replace(tzinfo=_dt.timezone.utc).astimezone(PT).strftime("%Y-%m-%d")


def token_counters() -> Dict[str, Any]:
    """Blockscout token counters: transfers_count + token_holders_count."""
    return cc._get(cc._session(), COUNTERS_URL, {})


# ---------------------------------------------------------------------------
# Registry file I/O
# ---------------------------------------------------------------------------

def load_registry(path: str) -> Dict[str, Dict[str, Any]]:
    reg: Dict[str, Dict[str, Any]] = {}
    if not os.path.exists(path):
        return reg
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            w = (row.get("wallet") or "").strip().lower()
            if not w:
                continue
            reg[w] = {"first": row.get("first_seen") or "",
                      "last": row.get("last_active") or "",
                      "tx": int(row.get("tx_count") or 0)}
    return reg


def save_registry(path: str, reg: Dict[str, Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(HEADER)
        for wallet in sorted(reg):
            e = reg[wallet]
            w.writerow([wallet, e["first"], e["last"], e["tx"]])
    os.replace(tmp, path)


def merge_registries(*regs: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Fusion lecture seule (min first / max last / somme tx)."""
    out: Dict[str, Dict[str, Any]] = {}
    for reg in regs:
        for w, e in reg.items():
            o = out.get(w)
            if o is None:
                out[w] = dict(e)
                continue
            if e["first"] and (not o["first"] or e["first"] < o["first"]):
                o["first"] = e["first"]
            if e["last"] > o["last"]:
                o["last"] = e["last"]
            o["tx"] += e["tx"]
    return out


def _update(reg: Dict[str, Dict[str, Any]], wallet: str, date: str) -> None:
    w = (wallet or "").strip().lower()
    if w in _SKIP or not w.startswith("0x"):
        return
    e = reg.get(w)
    if e is None:
        reg[w] = {"first": date, "last": date, "tx": 1}
        return
    if not e["first"] or date < e["first"]:
        e["first"] = date
    if date > e["last"]:
        e["last"] = date
    e["tx"] += 1


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

def _load_state() -> Dict[str, Any]:
    if not os.path.exists(STATE_JSON):
        return {}
    with open(STATE_JSON, encoding="utf-8") as f:
        return json.load(f)


def _save_state(state: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(STATE_JSON) or ".", exist_ok=True)
    tmp = STATE_JSON + ".tmp"
    state["updated_at"] = _dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=1)
    os.replace(tmp, STATE_JSON)


# ---------------------------------------------------------------------------
# Deep scan (present -> genese), resumable
# ---------------------------------------------------------------------------

def deep_scan() -> int:
    budget_s = float(os.environ.get("SCAN_MINUTES", "280")) * 60
    max_pages = int(os.environ.get("SCAN_MAX_PAGES", "0"))
    pause = float(os.environ.get("SCAN_PAUSE", "0.05"))

    state = _load_state()
    if state.get("done"):
        print("Deep scan deja termine (state.done=true) — rien a faire.", flush=True)
        return 0

    reg = load_registry(DEEP_CSV)
    print(f"Registre deep charge : {len(reg)} wallets. "
          f"Etat : pages={state.get('pages', 0)}, oldest={state.get('oldest_ts', '-')}",
          flush=True)

    try:
        counters = token_counters()
        total = int(counters.get("transfers_count") or 0)
        done_n = int(state.get("transfers", 0))
        print(f"Chaine : {counters.get('token_holders_count')} holders, "
              f"{total} transferts au total — deja traites : {done_n} "
              f"({100.0 * done_n / total if total else 0:.1f} %).", flush=True)
        if total:
            remaining_pages = max(0, (total - done_n)) // 50
            print(f"Estimation restante : ~{remaining_pages} pages "
                  f"(~{remaining_pages / 2.0 / 3600:.1f} h a ~2 pages/s).", flush=True)
    except Exception as e:
        print(f"counters warning: {e}", flush=True)

    session = cc._session()
    params: Dict[str, Any] = dict(state.get("next_page_params") or {})
    t0 = time.time()
    pages = 0
    transfers = 0
    oldest = state.get("oldest_ts", "")
    done = False

    while True:
        if max_pages and pages >= max_pages:
            print(f"Budget pages atteint ({max_pages}).", flush=True)
            break
        if time.time() - t0 > budget_s:
            print(f"Budget temps atteint ({budget_s / 60:.0f} min).", flush=True)
            break

        data = cc._get(session, cc.TRANSFERS_URL, params)
        items = data.get("items", [])
        for it in items:
            ts = cc._parse_ts(it.get("timestamp"))
            if ts is None:
                continue
            d = _pt_date(ts)
            _update(reg, ((it.get("from") or {}).get("hash") or ""), d)
            _update(reg, ((it.get("to") or {}).get("hash") or ""), d)
            transfers += 1
            oldest = d
        pages += 1

        nxt = data.get("next_page_params")
        state.update(next_page_params=nxt, oldest_ts=oldest,
                     pages=int(state.get("pages", 0)) + 1,
                     transfers=int(state.get("transfers", 0)) + len(items))
        # NB: state['pages'] cumule sur tous les runs ; `pages` = ce run.

        if pages % 200 == 0:
            rate = pages / max(1.0, time.time() - t0)
            print(f"    ... {pages} pages ce run ({rate:.1f}/s), "
                  f"{len(reg)} wallets, remonte a {oldest}", flush=True)
        if pages % SAVE_EVERY_PAGES == 0:
            save_registry(DEEP_CSV, reg)
            _save_state(state)
            print(f"    checkpoint sauvegarde ({len(reg)} wallets).", flush=True)

        if not nxt:
            done = True
            print("GENESE ATTEINTE — scan termine.", flush=True)
            break
        params = dict(nxt)
        if pause:
            time.sleep(pause)

    state["done"] = done
    state["runs"] = int(state.get("runs", 0)) + 1
    save_registry(DEEP_CSV, reg)
    _save_state(state)
    print(f"Run termine : {pages} pages, {transfers} transferts, "
          f"{len(reg)} wallets dans le registre, oldest={oldest}, done={done}, "
          f"run #{state['runs']}, duree {time.time() - t0:.0f}s.", flush=True)
    return 0


# ---------------------------------------------------------------------------
# Maintenance quotidienne (appelee par scraper.chain_run)
# ---------------------------------------------------------------------------

def update_from_records(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Merge les transferts du run chain quotidien dans wallet_registry_daily.csv.

    Retourne un resume pour le log : registry_wallets (taille du fichier daily),
    registry_new (nb de wallets jamais vus NULLE PART — seulement une fois le
    scan profond termine, sinon '' car on ne peut pas encore trancher).
    """
    out: Dict[str, Any] = {"registry_wallets": 0, "registry_new": ""}
    if not records:
        return out
    daily = load_registry(DAILY_CSV)
    before = set(daily)
    for r in records:
        d = _pt_date(r["ts"])
        _update(daily, r.get("from", ""), d)
        _update(daily, r.get("to", ""), d)
    save_registry(DAILY_CSV, daily)
    out["registry_wallets"] = len(daily)

    state = _load_state()
    if state.get("done"):
        deep = load_registry(DEEP_CSV)
        new = sorted(w for w in daily if w not in before and w not in deep)
        out["registry_new"] = len(new)
        if new:
            print(f"Nouveaux wallets (jamais vus depuis la genese) : {len(new)} "
                  f"— ex. {new[:5]}", flush=True)
    return out


def main() -> int:
    return deep_scan()


if __name__ == "__main__":
    sys.exit(main())
