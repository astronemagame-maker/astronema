"""
Wallet registry — deep CollectChain scan + daily maintenance + ARCHIVE.

But (finalites 2/5/6 de Preda) : dater la 1ere apparition on-chain de CHAQUE
wallet (~700k) ET archiver TOUS les transferts de la chaine (13,9 M) pendant
qu'on les telecharge — qui a achete quoi depuis la genese, activite whales,
burns — sans jamais mettre ca dans le Google Sheet.

Fichiers produits :

    data/wallet_registry_deep.csv   registre wallet -> first_seen/last_active
                                    (ecrit UNIQUEMENT par wallet-scan.yml).
    data/wallet_registry_daily.csv  idem, alimente par le run chain quotidien
                                    (scraper.chain_run -> update_from_records).
    archive/transfers_runNNN.csv.gz TOUS les transferts de la tranche NNN,
                                    uploade en GitHub Release "chain-archive"
                                    par le workflow (PAS commite dans le repo :
                                    pas de limite 100 Mo, repo leger).
                                    Colonnes : block, log_index, ts_utc,
                                    date_pt, kind, category, veve_uuid,
                                    edition, from, to.
                                    Dedup possible par (block, log_index)
                                    (doublons rares : reprise apres crash).

kind : mint / burn (vers 0x0 OU le coffre VeVe) / vault_mint (stock invendu
mint -> coffre) / listing (depot escrow) / market. Les wallets systeme sont
ARCHIVES (l'archive est brute) mais exclus du REGISTRE.

Dates en PT (America/Los_Angeles). Etat resumable dans
data/wallet_scan_state.json (next_page_params, done, runs, archived).

Env (scan profond) :
    SCAN_MINUTES    budget temps par run (defaut 280)
    SCAN_MAX_PAGES  budget pages par run (defaut 0 = illimite)
    SCAN_PAUSE      pause entre pages (defaut 0.05 s)
    SCAN_ARCHIVE    "false" pour desactiver l'archivage (defaut actif)
    SCAN_RESET      "true" = repartir de zero (ignore etat + registre existants)
"""

from __future__ import annotations

import csv
import datetime as _dt
import gzip
import io
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
ARCHIVE_DIR = os.environ.get("SCAN_ARCHIVE_DIR", "archive")

PT = ZoneInfo("America/Los_Angeles")
HEADER = ["wallet", "first_seen", "last_active", "tx_count"]
ARCHIVE_HEADER = ["block", "log_index", "ts_utc", "date_pt", "kind",
                  "category", "veve_uuid", "edition", "from", "to"]
SAVE_EVERY_PAGES = 2000          # checkpoint intermediaire (crash-safety)
COUNTERS_URL = f"{cc.API_BASE}/tokens/{cc.CONTRACT}/counters"

# Wallet coffre burn/vault VeVe (fallback si l'ancien collectchain ne l'a pas).
BURN_SINK = getattr(cc, "BURN_SINK", "0x39e3816a8c549ec22cd1a34a8cf7034b3941d8b1")
# Adresses systeme exclues du REGISTRE (mais presentes dans l'ARCHIVE).
_SKIP = {cc.ZERO, cc.MARKET_ESCROW, BURN_SINK, ""}


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
# Archive (tous les transferts, CSV.gz par tranche -> GitHub Release)
# ---------------------------------------------------------------------------

def _kind(frm: str, to: str) -> str:
    if frm == cc.ZERO:
        return "vault_mint" if to == BURN_SINK else "mint"
    if to == cc.ZERO or to == BURN_SINK:
        return "burn"
    if to == cc.MARKET_ESCROW:
        return "listing"
    return "market"


def _archive_row(it: Dict[str, Any], ts: _dt.datetime, d: str,
                 frm: str, to: str) -> List[Any]:
    total = it.get("total") or {}
    inst = (total.get("token_instance") or {}) if isinstance(total, dict) else {}
    cat, uuid = cc._categorise(inst)
    md = inst.get("metadata") or {}
    ed = md.get("edition") if isinstance(md, dict) else ""
    return [it.get("block_number"), it.get("log_index"),
            ts.strftime("%Y-%m-%d %H:%M:%S"), d, _kind(frm, to), cat, uuid,
            ed if ed not in (None, "") else "", frm, to]


def _flush_archive(path: str, rows: List[List[Any]], write_header: bool) -> int:
    """Append rows to a .csv.gz (concatenation de membres gzip = valide)."""
    if not rows:
        return 0
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    if write_header:
        w.writerow(ARCHIVE_HEADER)
    w.writerows(rows)
    with open(path, "ab") as f:
        f.write(gzip.compress(buf.getvalue().encode("utf-8")))
    return len(rows)


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
# Deep scan (present -> genese), resumable, avec archive
# ---------------------------------------------------------------------------

def deep_scan() -> int:
    budget_s = float(os.environ.get("SCAN_MINUTES", "280")) * 60
    max_pages = int(os.environ.get("SCAN_MAX_PAGES", "0"))
    pause = float(os.environ.get("SCAN_PAUSE", "0.05"))
    archive_on = os.environ.get("SCAN_ARCHIVE", "true").strip().lower() != "false"
    reset = os.environ.get("SCAN_RESET", "false").strip().lower() == "true"

    if reset:
        print("RESET demande : etat et registre repartent de zero "
              "(l'archivage couvrira toute la chaine).", flush=True)
        state: Dict[str, Any] = {}
        reg: Dict[str, Dict[str, Any]] = {}
    else:
        state = _load_state()
        if state.get("done"):
            print("Deep scan deja termine (state.done=true) — rien a faire.", flush=True)
            return 0
        reg = load_registry(DEEP_CSV)

    run_no = int(state.get("runs", 0)) + 1
    apath = os.path.join(ARCHIVE_DIR, f"transfers_run{run_no:03d}.csv.gz")
    if archive_on and os.path.exists(apath):
        os.remove(apath)   # rejeu du meme run apres crash : on repart proprement
    print(f"Registre deep charge : {len(reg)} wallets. "
          f"Etat : pages={state.get('pages', 0)}, oldest={state.get('oldest_ts', '-')}. "
          f"Archive : {'ON -> ' + apath if archive_on else 'OFF'}", flush=True)

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
    archived_run = 0
    abuf: List[List[Any]] = []
    header_pending = True
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
            frm = ((it.get("from") or {}).get("hash") or "").lower()
            to = ((it.get("to") or {}).get("hash") or "").lower()
            if archive_on:
                abuf.append(_archive_row(it, ts, d, frm, to))
            _update(reg, frm, d)
            _update(reg, to, d)
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
                  f"{len(reg)} wallets, {archived_run + len(abuf)} archives, "
                  f"remonte a {oldest}", flush=True)
        if pages % SAVE_EVERY_PAGES == 0:
            if archive_on:
                archived_run += _flush_archive(apath, abuf, header_pending)
                header_pending = False
                abuf = []
            save_registry(DEEP_CSV, reg)
            _save_state(state)
            print(f"    checkpoint sauvegarde ({len(reg)} wallets, "
                  f"{archived_run} transferts archives ce run).", flush=True)

        if not nxt:
            done = True
            print("GENESE ATTEINTE — scan termine.", flush=True)
            break
        params = dict(nxt)
        if pause:
            time.sleep(pause)

    if archive_on:
        archived_run += _flush_archive(apath, abuf, header_pending)
    state["done"] = done
    state["runs"] = run_no
    state["archived"] = int(state.get("archived", 0)) + archived_run
    save_registry(DEEP_CSV, reg)
    _save_state(state)
    print(f"Run termine : {pages} pages, {transfers} transferts "
          f"({archived_run} archives -> {apath if archive_on else '-'}), "
          f"{len(reg)} wallets, oldest={oldest}, done={done}, "
          f"run #{run_no}, duree {time.time() - t0:.0f}s.", flush=True)
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
