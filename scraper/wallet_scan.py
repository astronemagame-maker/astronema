# ⚠️ DEPOTS : astronema ET VeVePreda/scrapeur-veve   ·   CHEMIN : scraper/wallet_scan.py
#
# ⭐ FICHIER VOLONTAIREMENT PARTAGE ET IDENTIQUE dans ces deux depots (verifie
# byte-a-byte le 29/07/2026). La question n'est donc pas « ou va-t-il ? » mais
# « est-il identique PARTOUT ? ». Deposer une version dans UN SEUL des deux
# creerait exactement la divergence silencieuse qu'on repare ici.
#
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
# 🆕 11e colonne (05/08/2026) — `token_id`, L'IDENTITE DE L'EXEMPLAIRE.
# ⭐⭐⭐ ELLE SAUVE EXACTEMENT LES LIGNES QUE `veve_uuid` NE PEUT PAS SAUVER.
#
# `_categorise(inst)` lit l'uuid dans l'ADRESSE DE L'IMAGE de la metadonnee. Au
# MINT, cette metadonnee n'est pas encore attachee : pas d'image, donc pas
# d'uuid, donc une ligne anonyme. Mesure du 05/08 sur l'archive locale :
#
#     167 159 transferts CollectChain sans veve_uuid, dont
#         103 672 mint          (61,9 %)
#          63 386 vault_mint    (37,9 %)  <- le stock invendu, matiere du 🔥 BURN
#             101 tout le reste ( 0,1 %)
#
# 99,96 % sont des mints. Or `total.token_id` est un champ du TRANSFERT, pas de
# la metadonnee : il est present sur ces lignes-la aussi. Avec lui, la carte
# `holders` (token_id -> veve_uuid, qui traduit deja 99,25 % des token_id IMX)
# les rend identifiables.
# ⭐⭐ UNE LIGNE ANONYME N'EST PAS UNE LIGNE SANS IDENTITE : C'EST UNE LIGNE
# DONT ON A LU LE MAUVAIS CHAMP.
#
# ⛔ EN FIN, jamais au milieu : `merge_transfers.py` (fanablefrance/jetonveve)
# lit ce format par POSITION (`p[0]`..`p[9]`, garde `len(p) < 10`). Une
# insertion ne leverait aucune erreur chez lui — elle decalerait les valeurs,
# dans un autre depot et plus tard.
# ⭐⭐ UN FORMAT PARTAGE PAR TROIS DEPOTS NE SE MODIFIE QU'EN FIN.
#
# ⚠️ MEME COLONNE, MEME PLACE que `scrapeur-veve/scraper/chain_run.py` (lot 64).
# Les deux ecrivent le MEME format et doivent rester alignes : c'est la raison
# d'etre de l'alignement declare en tete de `chain_run.ARCHIVE_HEADER`.
#
# ⛔ CE LOT NE REMPLIT RIEN A LUI SEUL : l'archive deja publiee garde ses
# colonnes. Le token_id n'apparait qu'au PROCHAIN SCAN PROFOND.
ARCHIVE_HEADER = ["block", "log_index", "ts_utc", "date_pt", "kind",
                  "category", "veve_uuid", "edition", "from", "to", "token_id"]
SAVE_EVERY_PAGES = 2000          # checkpoint intermediaire (crash-safety)
COUNTERS_URL = f"{cc.API_BASE}/tokens/{cc.CONTRACT}/counters"

# Wallet coffre burn/vault VeVe (fallback si l'ancien collectchain ne l'a pas).
BURN_SINK = getattr(cc, "BURN_SINK", "0x39e3816a8c549ec22cd1a34a8cf7034b3941d8b1")
# Adresses systeme exclues du REGISTRE (mais presentes dans l'ARCHIVE).
_SKIP = (set(getattr(cc, "SYSTEM_WALLETS", ()))
         | {cc.ZERO, cc.MARKET_ESCROW, BURN_SINK, ""})
# ---------------------------------------------------------------------------
# 🔴🔴 CE REPLI ECHOUAIT OUVERT — corrige le 29/07/2026.
#
# Avant : `_DISTRIB = frozenset(getattr(cc, "DISTRIB_WALLETS", ()))`.
# Ecrit comme un garde-fou (« au cas ou collectchain serait vieux »), c'etait en
# realite un interrupteur silencieux : la constante manquait VRAIMENT dans le
# collectchain de ce depot, donc `_DISTRIB` valait frozenset(), la branche
# `system_transfer` de `_kind()` etait MORTE, et toutes les livraisons VeVe sont
# parties dans l'archive en `kind="market"`.
# Cout mesure le 29/07 sur l'entrepot : 216 838 transferts mal etiquetes,
# 26,9 % de tout le « market » de l'ere CollectChain. Aucune erreur, aucun log.
#
# ⭐⭐ UN REPLI DOIT CRIER CE QU'IL REMPLACE. Un `getattr(..., ())` transforme
# une dependance absente en comportement normal — c'est la meme famille que le
# `except Exception` qui rend 429 et timeout egaux.
# ---------------------------------------------------------------------------
_DISTRIB = frozenset(getattr(cc, "DISTRIB_WALLETS", ()))
if not _DISTRIB:
    raise RuntimeError(
        "collectchain.DISTRIB_WALLETS est absent ou vide : les livraisons VeVe "
        "seraient archivees en kind='market' (defaut du 29/07, 216 838 lignes). "
        "Deposer le collectchain.py a jour dans CE depot avant de relancer.")


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
    if frm in _DISTRIB or to in _DISTRIB:
        return "system_transfer"
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
            ed if ed not in (None, "") else "", frm, to,
            # ⭐ `total.token_id`, PAS `inst.metadata` : c'est un champ du
            # transfert. Il survit donc aux mints, ou la metadonnee est encore
            # vide et ou `uuid` sort vide juste au-dessus.
            str(total.get("token_id") or "")]


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
    # 🔴🔴 13/08/2026 — 400 000 TRANSFERTS ONT DISPARU PAR CE NOM DE FICHIER.
    # `run_no` derive de state["runs"], qui n'est ecrit qu'a la SORTIE de la
    # boucle. Le curseur de pagination, lui, est sauve a chaque checkpoint.
    # ⇒ deux runs peuvent avancer dans la chaine en portant le MEME numero,
    # donc ecrire le MEME nom de tranche, et l'upload `--clobber` fait
    # disparaitre la premiere SANS AUCUNE ERREUR.
    # Mesure du 13/08 : 80 runs GitHub, 33 tranches, 400 000 lignes absentes
    # sur les blocs 5138-5255 (contigus) — voir MESURE-trou-scan-13-08-2026.md.
    #
    # ⭐ Le tag est fourni par l'appelant (identifiant de run GitHub, unique
    # par construction). Sans tag on retombe sur l'ancien nom : le module
    # reste lancable a la main sans rien casser.
    _brut = os.environ.get("SCAN_ARCHIVE_TAG", "").strip()
    tag = "".join(c for c in _brut if c.isalnum() or c in "_-")[:40]
    apath = os.path.join(
        ARCHIVE_DIR,
        f"transfers_{tag}.csv.gz" if tag else f"transfers_run{run_no:03d}.csv.gz")
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
            # 🔴 L'ESTIMATION DIVISAIT PAR 2.0 EN DUR (corrige le 05/08/2026).
            #
            # Le scan IMPRIME sa cadence reelle toutes les 200 pages — il la
            # connait donc. L'estimation, elle, supposait 2 pages/s pour
            # toujours. Mesure du run #47 : cadence reelle **1,1 p/s**, ETA
            # annoncee **33,3 h**, ETA vraie **60 h**. Un facteur 2 sur une
            # attente de deux jours, et rien pour le signaler.
            #
            # ⭐⭐ **UNE ESTIMATION QUI N'UTILISE PAS LA MESURE QU'ELLE A SOUS
            # LA MAIN EST UNE PROMESSE, PAS UNE PREVISION.**
            #
            # ⚠️ La cadence n'est pas connue AU DEMARRAGE de ce run — on repart
            # donc de celle du run PRECEDENT, memorisee dans l'etat. Au tout
            # premier run il n'y a rien : on garde 2.0, et on le DIT.
            # ⭐ La valeur de repli est ANNONCEE comme telle : c'est ce qui
            # manquait. Un chiffre par defaut qui ne se presente pas comme un
            # defaut se lit comme une mesure.
            cadence = float(state.get("cadence_p_s") or 0.0)
            if cadence > 0:
                origine = f"cadence MESUREE au run precedent : {cadence:.2f} p/s"
            else:
                cadence, origine = 2.0, "AUCUNE mesure encore — repli a 2 p/s"
            heures = remaining_pages / cadence / 3600
            print(f"Estimation restante : ~{remaining_pages} pages "
                  f"(~{heures:.1f} h — {origine}).", flush=True)
            # ⭐ Le nombre de RUNS est ce qui interesse vraiment : c'est lui
            # qu'on compare au garde-fou. L'afficher evite de le recalculer de
            # tete a chaque fois — et de decouvrir trop tard qu'on le frole.
            par_run = budget_s * cadence
            if par_run > 0:
                restants = remaining_pages / par_run
                alerte = "  ⚠️ GARDE-FOU 40 RUNS EN VUE" if restants > 30 else ""
                print(f"  soit ~{restants:.1f} run(s) de "
                      f"{budget_s/60:.0f} min a cette cadence.{alerte}",
                      flush=True)
    except Exception as e:
        print(f"counters warning: {e}", flush=True)

    session = cc._session()
    params: Dict[str, Any] = dict(state.get("next_page_params") or {})
    t0 = time.time()
    pages = 0
    transfers = 0
    archived_run = 0
    # 🔴 13/08/2026 — `state["transfers"]` compte `len(items)` AVANT le filtre
    # des timestamps nuls, alors que le tampon d'archive se remplit APRES.
    # ⭐⭐ LES DEUX NE COMPTENT PAS LA MEME CHOSE, MEME QUAND TOUT VA BIEN :
    # un controle « transfers == archived » rougirait pour une raison legitime.
    # D'ou ce troisieme compteur, pose exactement la ou le tampon se remplit —
    # c'est lui, et lui seul, qui se compare a ce qui est ecrit.
    archivables = 0
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
                archivables += 1   # 🔴 compte AU MEME ENDROIT que le tampon
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

        # 🔴🔴 INVARIANT LOCAL — tout ce qui est entre dans le tampon doit etre
        # ressorti sur le disque. Il ne remplace pas le controle d'apres-upload
        # (l'ecrasement du 13/08 s'est produit APRES cette ligne, dans la
        # Release) : il ferme l'autre moitie du chemin.
        if archived_run != archivables:
            raise SystemExit(
                f"⛔ ARCHIVE INCOMPLETE : {archivables} lignes mises en tampon, "
                f"{archived_run} ecrites dans {apath} "
                f"(manque {archivables - archived_run}). "
                "Etat NON sauvegarde — le prochain run repartira du meme point.")

    state["done"] = done
    state["runs"] = run_no
    state["archived"] = int(state.get("archived", 0)) + archived_run
    # ⚠️ CLE NEUVE SUR UN ETAT ANCIEN : sans cette amorce, `archivable`
    # partirait de 0 face a un `archived` deja a 13 764 444, et l'invariant
    # serait faux des le premier run. On l'aligne une seule fois.
    if "archivable" not in state:
        state["archivable"] = int(state.get("archived", 0)) - archived_run
    state["archivable"] = int(state["archivable"]) + archivables
    # 🆕 LA CADENCE DE CE RUN, POUR L'ESTIMATION DU SUIVANT (05/08/2026).
    # ⭐ On memorise la cadence du run QUI VIENT DE FINIR, pas une moyenne
    # depuis le debut : la cadence de ce scan s'est effondree de 3,6 a 1,1 p/s
    # en atteignant la genese du 28/01. Une moyenne aurait lisse exactement le
    # phenomene qu'on veut voir. ⭐⭐ *Une moyenne sur une grandeur qui derive
    # decrit un regime qui n'existe plus.*
    duree = max(1.0, time.time() - t0)
    if pages:
        state["cadence_p_s"] = round(pages / duree, 3)
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
