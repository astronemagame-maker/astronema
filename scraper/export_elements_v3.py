# ⚠️ DEPOT : VeVePreda/scrapeur-veve   ·   CHEMIN : scraper/export_elements_v3.py
# Le projet vit sur 6 depots et DEUX comptes GitHub. Un fichier depose au
# mauvais endroit ne provoque aucune erreur : il dort.

"""🌉 LE PONT v3 — elements.csv fabrique depuis la CHAINE (CollectChain).

Suite du spike GO-catalogue (23/07). L'IDENTITE du catalogue vient de la
metadata on-chain d'un transfert (`total.token_instance.metadata`), pas du
tracker communautaire :

    name, category, rarity, edition_type, supply, brand, licensor  <- CHAINE

Ce que la chaine NE porte PAS reste OFF-CHAIN et est REPORTE de l'export
officiel (data/elements.csv) tel quel — donc identique, donc 0 ecart au
comparateur :

    series_uuid, first_public, listings, note, atl, atl_date, ath, ath_date

    (spike : series_uuid absent de la chaine ; first_public = first_available_edition
     un NUMERO, pas la dropDate ; aucun prix on-chain.)

Ce module ecrit `data/elements_v3.csv` et NE TOUCHE PAS a data/elements.csv.
La bascule se juge au comparateur (scraper.compare_elements, pilote par
ELEMENTS_V2=data/elements_v3.csv) : identite a 0 sur plusieurs jours, comme le
pont elements. export_elements.py (v1) et export_elements_v2.py (tracker)
restent en repli.

En-tete OCTET POUR OCTET identique a v1/v2.

--- ALIMENTATION ---
La metadata catalogue n'est PAS dans l'archive des transferts (schema reduit).
v3 doit la MOISSONNER en direct : un echantillon de metadata (le plus recent)
par veve_uuid. La moisson pleine (~26 800 types) = un run GitHub, comme tout
collecteur (le sandbox ne fait que des sondes ciblees). `collapse()` accepte
n'importe quel iterable de transferts bruts (API live, JSONL moissonne...).
"""

from __future__ import annotations

import csv
import os
import re
import sys
from typing import Any, Dict, Iterable, List, Optional, Tuple

# En-tete identique v1/v2/v3.
ENTETE = ["veve_uuid", "series_uuid", "name", "category", "rarity",
          "edition_type", "supply", "first_public", "listings", "note",
          "brand", "licensor", "atl", "atl_date", "ath", "ath_date"]

CSV_V3 = os.environ.get("ELEMENTS_V3", "data/elements_v3.csv")
CSV_OFFICIEL = os.environ.get("ELEMENTS_CSV", "data/elements.csv")
# Etat de reprise (mode profond) : curseur de pagination + flag balayage complet.
STATE_V3 = os.environ.get("ELEMENTS_V3_STATE", "data/elements_v3_state.json")
SUPPLY_MAX = int(os.environ.get("ELEMENTS_SUPPLY_MAX", "0"))   # 0 = tout

# Les 8 colonnes OFF-CHAIN reportees de l'officiel (cf. docstring).
OFFCHAIN_COLS = ["series_uuid", "first_public", "listings", "note",
                 "atl", "atl_date", "ath", "ath_date"]

# `collectible_type_image.<veve_uuid>` ou `comic_cover.<veve_uuid>` — le 1er uuid
# apres le prefixe EST le veve_uuid du catalogue (le 2e n'est PAS le series_uuid,
# verifie 23/07 : il differe du series_uuid officiel -> series_uuid reste off-chain).
_UUID_RE = re.compile(
    r"(collectible_type_image|comic_cover)\."
    r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
    re.I,
)


def _norm_rarity(r: Any) -> str:
    """'Ultra Rare' -> 'ULTRA_RARE' ; 'Rare' -> 'RARE' (format de l'officiel)."""
    s = str(r or "").strip()
    return re.sub(r"[\s\-]+", "_", s).upper()


def _num(x) -> int:
    try:
        return int(float(str(x).replace(",", ".").replace(" ", "") or 0))
    except (TypeError, ValueError):
        return 0


def catalogue_from_instance(inst: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Un `token_instance` -> les champs catalogue tires de la CHAINE.

    Retourne None si l'instance n'est pas rattachable (pas de veve_uuid ET pas
    de metadata exploitable). category deduite de l'URL image, sinon des cles
    de metadata (comics : comicNumber/artists ; collectibles : editionType)."""
    if not isinstance(inst, dict):
        return None
    md = inst.get("metadata") or {}
    if not isinstance(md, dict):
        md = {}
    img = inst.get("image_url") or inst.get("media_url") or ""
    m = _UUID_RE.search(img)
    if m:
        cat = "collectible" if m.group(1).lower().startswith("collectible") \
            else "comic"
        uuid = m.group(2).lower()
    else:
        # Repli : deviner la categorie par les cles de metadata (uuid inconnu).
        if any(k in md for k in ("comicNumber", "coverArtists", "artists")):
            cat, uuid = "comic", ""
        elif any(k in md for k in ("editionType", "rarity")):
            cat, uuid = "collectible", ""
        else:
            return None
    if not uuid and not md:
        return None

    rarity = _norm_rarity(md.get("rarity"))
    total_ed = _num(md.get("totalEditions"))
    series = str(md.get("series") or "").strip()

    if cat == "comic":
        comic_no = str(md.get("comicNumber") or "").strip()
        start_year = str(md.get("startYear") or "").strip()
        # name = "{serie} #{numero} ({annee})" (calibre sur l'officiel 23/07).
        name = f"{series} #{comic_no}"
        if start_year:
            name = f"{name} ({start_year})"
        edition_type = comic_no                  # comics : edition_type = comicNumber
        brand = series                           # comics : brand = la serie
        licensor = str(md.get("publisher") or "").strip()  # comics : licensor = publisher
    else:
        name = str(md.get("name") or "").strip()
        et = str(md.get("editionType") or "").strip()
        edition_type = "" if et in ("0", "0.0") else et.upper()
        brand = str(md.get("brand") or "").strip()
        licensor = str(md.get("licensor") or "").strip()

    return {
        "veve_uuid": uuid,
        "category": cat,
        "name": name,
        "rarity": rarity,
        "edition_type": edition_type,
        "supply": total_ed,
        "brand": brand,
        "licensor": licensor,
        "series": series,       # sert au MAX-par-serie des comics
    }


def _order(item: Dict[str, Any]) -> Tuple[int, int]:
    """Cle de recence d'un transfert brut : (block, log_index)."""
    b = item.get("block_number")
    b = b if isinstance(b, int) else _num(b)
    li = item.get("log_index")
    li = li if isinstance(li, int) else _num(li)
    return (b, li)


def collapse(transfers: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Iterable de transferts BRUTS (API) -> {veve_uuid: catalogue le plus recent}.

    « Derniere metadata par item » : si un item reapparait, on garde celle du
    transfert au (block, log_index) le plus GRAND (metadata la plus a jour)."""
    best: Dict[str, Dict[str, Any]] = {}
    best_ord: Dict[str, Tuple[int, int]] = {}
    for t in transfers:
        inst = (((t.get("total") or {}).get("token_instance")) or {})
        cat = catalogue_from_instance(inst)
        if not cat or not cat["veve_uuid"]:
            continue
        uid = cat["veve_uuid"]
        o = _order(t)
        if uid not in best or o >= best_ord[uid]:
            best[uid] = cat
            best_ord[uid] = o
    return best


def lire_officiel(chemin: str) -> Dict[str, Dict[str, str]]:
    """{veve_uuid: ligne officielle} — source des colonnes OFF-CHAIN reportees."""
    out: Dict[str, Dict[str, str]] = {}
    if not os.path.exists(chemin):
        return out
    with open(chemin, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            uid = (r.get("veve_uuid") or "").strip()
            if uid:
                out[uid] = r
    return out


def construire_v3(catalogue: Dict[str, Dict[str, Any]],
                  officiel: Dict[str, Dict[str, str]]) -> List[List]:
    """Le catalogue on-chain + les colonnes off-chain reportees -> lignes ENTETE."""
    # ⭐ Tirage d'un COMIC = MAX par SERIE (comme v1/v2). CLE DE GROUPE = le
    # `series_uuid` OFFICIEL qu'on reporte deja (fin), avec repli sur la chaine
    # de serie on-chain. Grouper sur la chaine `series` seule SUR-AGREGEAIT
    # (plusieurs series_uuid partagent un meme libelle -> MAX gonfle : 30000 vs
    # 7500, cf. 1er rapport 23/07). La cle officielle recolle au decoupage v1.
    def _serie_key(uid: str, c: Dict[str, Any]) -> str:
        su = (officiel.get(uid, {}).get("series_uuid") or "").strip()
        return su or ("~" + c["series"])       # ~ = repli libelle si hors officiel

    max_par_serie: Dict[str, int] = {}
    for uid, c in catalogue.items():
        if c["category"] == "comic" and c["supply"]:
            k = _serie_key(uid, c)
            if k not in ("", "~"):
                max_par_serie[k] = max(max_par_serie.get(k, 0), c["supply"])

    rows: List[List] = []
    for uid, c in catalogue.items():
        off = officiel.get(uid, {})
        if c["category"] == "comic":
            supply = max_par_serie.get(_serie_key(uid, c), c["supply"])
        else:
            supply = c["supply"]
        if SUPPLY_MAX and supply and supply > SUPPLY_MAX:
            continue
        rows.append([
            uid,
            (off.get("series_uuid") or "").strip(),   # OFF-CHAIN reporte
            c["name"],
            c["category"],
            c["rarity"],
            c["edition_type"],
            supply if supply else "",
            (off.get("first_public") or "").strip(),  # OFF-CHAIN reporte
            (off.get("listings") or "").strip(),       # OFF-CHAIN reporte
            (off.get("note") or "").strip(),           # OFF-CHAIN reporte
            c["brand"],
            c["licensor"],
            (off.get("atl") or "").strip(),            # OFF-CHAIN reporte
            (off.get("atl_date") or "").strip(),
            (off.get("ath") or "").strip(),
            (off.get("ath_date") or "").strip(),
        ])
    rows.sort(key=lambda l: (l[3], l[6] if l[6] != "" else 0, l[2]))
    return rows


def ecrire(rows: List[List], chemin: str) -> None:
    os.makedirs(os.path.dirname(chemin) or ".", exist_ok=True)
    with open(chemin, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(ENTETE)
        w.writerows(rows)


def charger_graine(chemin: str) -> Dict[str, List]:
    """La graine (CSV_V3 d'un run precedent) -> {veve_uuid: ligne ENTETE}.
    Chargee EN MEMOIRE avant la moisson : le flush de secours ecrase ensuite
    CSV_V3 avec la tranche courante, donc on ne peut plus la relire du disque."""
    out: Dict[str, List] = {}
    if not os.path.exists(chemin):
        return out
    with open(chemin, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            uid = (r.get("veve_uuid") or "").strip()
            if uid:
                out[uid] = [r.get(c, "") for c in ENTETE]
    return out


def fusion(rows: List[List], graine: Dict[str, List]) -> List[List]:
    """rows du run + lignes de la graine ABSENTES du run (types non revus) — on
    ne reperd JAMAIS un type. Le run courant fait foi pour un uuid revu."""
    vus = {r[0] for r in rows}
    out = list(rows) + [g for uid, g in graine.items() if uid not in vus]
    out.sort(key=lambda l: (l[3], _num(l[6]) if l[6] != "" else 0, l[2]))
    return out


def reattacher_offchain(rows: List[List],
                        officiel: Dict[str, Dict[str, str]]) -> List[List]:
    """Rattache le OFF-CHAIN (series_uuid, first_public, listings, note, atl/ath)
    depuis l'officiel FRAIS, pour CHAQUE ligne connue de l'officiel — quelle que
    soit sa provenance (chaine, graine, comblage). Indispensable apres un
    rapatriement d'un catalogue CHAINE-ONLY (off-chain vide) : sinon les lignes
    dormantes garderaient un off-chain vide et pollueraient le verdict d'identite.
    Sans officiel (ex. run astronema chaine-only) : no-op."""
    if not officiel:
        return rows
    idx = {c: ENTETE.index(c) for c in OFFCHAIN_COLS}
    for r in rows:
        o = officiel.get(r[0])
        if not o:
            continue
        for c in OFFCHAIN_COLS:
            r[idx[c]] = (o.get(c) or "").strip()
    return rows


def combler_depuis_officiel(rows: List[List],
                            officiel: Dict[str, Dict[str, str]]) -> List[List]:
    """COMPLETUDE : tout uuid de l'officiel (elements.csv) pas encore couvert par
    la chaine est repris TEL QUEL (tracker). elements_v3 est alors COMPLET des le
    1er run — chaine pour l'actif, tracker pour la traine DORMANTE (jamais tradee
    recemment). Les runs profonds convertissent progressivement la traine en
    chaine (le uuid passe alors dans `rows`, il fait foi). Auto-cicatrisant."""
    vus = {r[0] for r in rows}
    ajout = 0
    for uid, r in officiel.items():
        if uid in vus:
            continue
        rows.append([r.get(c, "") for c in ENTETE])
        ajout += 1
    if ajout:
        print(f"  completude : +{ajout} type(s) DORMANT(s) repris de l'officiel "
              f"(tracker) — catalogue complet ({len(rows)}).", flush=True)
    rows.sort(key=lambda l: (l[3], _num(l[6]) if l[6] != "" else 0, l[2]))
    return rows


def lire_state(chemin: str) -> Dict[str, Any]:
    """Etat de reprise : {cursor, swept, oldest, ...} — {} si absent/illisible."""
    import json
    if not os.path.exists(chemin):
        return {}
    try:
        with open(chemin, encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:                                          # noqa: BLE001
        return {}


def ecrire_state(chemin: str, state: Dict[str, Any]) -> None:
    import json
    os.makedirs(os.path.dirname(chemin) or ".", exist_ok=True)
    with open(chemin, "w", encoding="utf-8") as f:
        json.dump(state, f)


def main() -> int:
    """Moissonne la metadata chaine, reporte l'off-chain, ecrit elements_v3.csv.

    La moisson pleine tourne en GitHub Actions (le sandbox ne joint pas l'API en
    volume). Ici on branche `fetch_transfers` de collectchain, qui pagine +
    reprend proprement ; ELEMENTS_V3_CUTOFF borne la profondeur du balayage."""
    try:
        from scraper import collectchain as cc
    except Exception as e:                                     # noqa: BLE001
        print(f"⛔ import collectchain impossible ({e}).", file=sys.stderr)
        return 2

    import datetime as _dt
    import time as _time
    # ── MODE ──────────────────────────────────────────────────────────────
    # 'tete'   : repart du sommet, arret sur couverture (plateau). Entretien
    #            quotidien : attrape les nouveaux drops, rafraichit la metadata.
    # 'profond': REPREND au curseur du state (descend plus bas sans re-scanner
    #            le haut), plateau DESARME. Repete jusqu'a swept -> univers COMPLET.
    mode = os.environ.get("ELEMENTS_V3_MODE", "tete").strip().lower()
    profond = mode == "profond"
    state = lire_state(STATE_V3)
    start_params = None
    if profond and state.get("cursor") and not state.get("swept"):
        start_params = state["cursor"]
        print(f"  mode PROFOND : reprise au curseur du state "
              f"(deja descendu jusqu'a {state.get('oldest', '?')}).", flush=True)
    elif profond and state.get("swept"):
        print("  mode PROFOND : state deja 'swept' (univers complet balaye) — "
              "on repart du sommet pour rafraichir.", flush=True)

    # profond : fenetre large par defaut (on veut tout) + plateau desarme.
    # `... or default` : un env ABSENT *ou VIDE* (input workflow non renseigne)
    # retombe sur le defaut du mode — sinon int("") planterait.
    days = int(os.environ.get("ELEMENTS_V3_LOOKBACK_DAYS")
               or ("3650" if profond else "120"))
    cutoff = _dt.datetime.utcnow() - _dt.timedelta(days=days)
    plateau = int(os.environ.get("ELEMENTS_V3_PLATEAU_PAGES")
                  or ("0" if profond else "300"))
    # ⭐ GARANTIE ANTI-TIMEOUT : budget-temps INTERNE < timeout du job GitHub
    # (300 min). On s'arrete PROPREMENT avant le couperet -> l'ecriture + l'upload
    # Release s'executent (sur un timeout GitHub, meme les steps `if: always()`
    # sont sautes). Le reste se recolte au dispatch suivant (accumulation).
    budget_min = int(os.environ.get("ELEMENTS_V3_TIME_BUDGET_MIN") or "240")
    deadline = _time.monotonic() + budget_min * 60 if budget_min > 0 else None
    flush_every = int(os.environ.get("ELEMENTS_V3_FLUSH_EVERY") or "200")

    # officiel + GRAINE charges AVANT la moisson. La graine en memoire est LA
    # reference d'accumulation : le flush ecrase ensuite CSV_V3 avec la tranche
    # courante, donc on ne peut plus la relire du disque (bug evite en profond).
    officiel = lire_officiel(CSV_OFFICIEL)
    accumule = os.environ.get("ELEMENTS_V3_ACCUMULATE", "").strip() in (
        "1", "true", "oui")
    graine = charger_graine(CSV_V3) if accumule else {}
    if graine:
        print(f"  graine chargee en memoire : {len(graine)} types (reference "
              f"d'accumulation).", flush=True)

    def _flush(best: Dict[str, Dict[str, Any]]) -> None:
        """Sauvegarde de secours : ecrit le CSV COMPLET (tranche courante FUSION
        graine) en cours de route -> meme une chute hors timeout ne perd rien."""
        try:
            partiel = construire_v3(best, officiel)
            if partiel:
                ecrire(fusion(partiel, graine), CSV_V3)
        except Exception as e:                                 # noqa: BLE001
            print(f"    (flush de secours ignore : {e})", file=sys.stderr)

    arret = "curseur/fin (plateau desarme)" if plateau == 0 \
        else f"couverture ({plateau} pages sans nouveau)"
    print(f"Moisson metadata chaine [{mode}] depuis {cutoff:%Y-%m-%d} · arret sur "
          f"{arret} ou budget-temps ({budget_min} min) · flush /{flush_every} "
          f"pages …", flush=True)

    catalogue, meta = harvest(cc, cutoff, plateau, deadline=deadline,
                              flush=_flush, flush_every=flush_every,
                              start_params=start_params)
    # tete : la tranche DOIT voir l'univers actif -> <50 = quelque chose a casse.
    # profond : une tranche est bornee par le budget, elle peut etre petite ;
    # on rejette seulement une tranche VIDE (API muette). L'accumulation + le
    # curseur completent le reste au fil des dispatches.
    seuil = 1 if profond else 50
    if len(catalogue) < seuil:
        print(f"⛔ moisson trop maigre ({len(catalogue)} types) — rien d'ecrit.",
              file=sys.stderr)
        return 3
    rows = construire_v3(catalogue, officiel)
    if not rows:
        print("⛔ 0 ligne — rien d'ecrit.", file=sys.stderr)
        return 3
    # ACCUMULATION : fusion avec la graine EN MEMOIRE (chargee avant le flush).
    # Les types d'un run precedent PAS revus cette fois sont conserves.
    if accumule:
        rows = fusion(rows, graine)
    # COMPLETUDE : combler la traine dormante depuis l'officiel (defaut ON —
    # ELEMENTS_V3_COMBLER_OFFICIEL=0 pour un run chaine-pur). Decision Preda 23/07 :
    # inutile de balayer jusqu'a 2021 pour les dormants, le tracker les porte.
    if os.environ.get("ELEMENTS_V3_COMBLER_OFFICIEL", "1").strip() != "0":
        rows = combler_depuis_officiel(rows, officiel)
    # OFF-CHAIN toujours frais depuis l'officiel (répare un catalogue chaîne-only
    # rapatrié dont l'off-chain serait vide). No-op si pas d'officiel (astronema).
    rows = reattacher_offchain(rows, officiel)
    ecrire(rows, CSV_V3)

    # ── STATE de reprise : curseur pour descendre plus bas au prochain profond.
    if profond:
        new_state = {
            "cursor": meta["cursor"],
            "swept": bool(meta["swept"]),
            "oldest": meta["oldest"] or state.get("oldest", ""),
            "pages_last": meta["pages"],
            # compteur de dispatches (garde-fou d'auto-relance cote workflow).
            "runs": int(state.get("runs", 0)) + 1,
            "updated": _dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        }
        ecrire_state(STATE_V3, new_state)
        if meta["swept"]:
            print("  ✅ BALAYAGE INTEGRAL TERMINE (swept) — l'univers on-chain "
                  "est couvert. Passe en mode 'tete' pour l'entretien.", flush=True)
        else:
            print(f"  ↪ state sauve : curseur pose (descendu jusqu'a "
                  f"{new_state['oldest']}). Relancer en PROFOND continue plus "
                  f"bas.", flush=True)

    nc = sum(1 for r in rows if r[3] == "comic")
    print(f"🌉 v3 : {len(rows)} elements ({nc} comics, {len(rows) - nc} "
          f"collectibles) · +{meta['types']} vus ce run -> {CSV_V3}", flush=True)
    return 0


def harvest(cc, cutoff, plateau_pages: int = 300, deadline=None,
            flush=None, flush_every: int = 0, start_params=None
            ) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    """Pagine /transfers newest-first et COLLECTE la metadata catalogue au vol,
    avec des garde-fous pour ne jamais balayer des millions de lignes pour rien
    NI perdre une recolte sur un couperet :

      * REPRISE PAR CURSEUR : `start_params` = curseur d'ou REPRENDRE (mode
        profond) au lieu du sommet -> des dispatches successifs descendent PLUS
        BAS sans re-scanner le haut. None = repart du sommet.
      * arret sur COUVERTURE : si `plateau_pages` pages defilent sans AUCUN
        nouveau veve_uuid -> arret (0 = DESARME, pour un balayage integral).
      * arret sur BUDGET-TEMPS : `deadline` (time.monotonic) -> arret PROPRE
        avant le timeout du job.
      * flush de SECOURS tous les `flush_every` pages ; plafond dur
        `ELEMENTS_V3_MAX_PAGES` ; arret sur CUTOFF.

    Retourne (best, meta). meta = {cursor, swept, pages, oldest, types} :
      * `cursor` = curseur pour REPRENDRE plus bas au prochain dispatch (None si
        `swept`), pour ne pas re-scanner le sommet.
      * `swept` = True quand toute l'histoire est balayee (plus de page suivante).
    Journalise sa progression (sinon un run long semble plante)."""
    import time
    session = cc._session()
    params: Dict[str, Any] = dict(start_params) if start_params else {}
    pages = 0
    max_pages = int(os.environ.get("ELEMENTS_V3_MAX_PAGES") or "20000")
    best: Dict[str, Dict[str, Any]] = {}
    best_ord: Dict[str, Tuple[int, int]] = {}
    since_new = 0
    newest_date = ""
    swept = False
    last_nxt = start_params      # si 0 page traitee, on reprend au meme point
    oldest = ""
    while pages < max_pages:
        data = cc._get(session, cc.TRANSFERS_URL, params)
        items = data.get("items", [])
        if not items:
            swept = True
            print(f"  ✓ plus aucun transfert -> BALAYAGE COMPLET "
                  f"({len(best)} types).", flush=True)
            break
        new_this = 0
        stop = False
        for it in items:
            ts = cc._parse_ts(it.get("timestamp"))
            if ts is not None and ts < cutoff:
                stop = True
                break
            if not newest_date and it.get("timestamp"):
                newest_date = str(it["timestamp"])[:10]
            inst = (((it.get("total") or {}).get("token_instance")) or {})
            cat = catalogue_from_instance(inst)
            if not cat or not cat["veve_uuid"]:
                continue
            uid = cat["veve_uuid"]
            o = _order(it)
            if uid not in best:
                new_this += 1
            if uid not in best or o >= best_ord[uid]:
                best[uid] = cat
                best_ord[uid] = o
        pages += 1
        since_new = 0 if new_this else since_new + 1
        try:
            oldest = str(items[-1].get("timestamp"))[:10]
        except Exception:
            pass
        nxt = data.get("next_page_params")
        last_nxt = nxt                       # curseur de la page SUIVANTE
        if pages % 25 == 0:
            print(f"    … {pages} pages · {len(best)} types · "
                  f"{since_new} page(s) sans nouveau · jusqu'a {oldest}",
                  flush=True)
        if flush and flush_every and pages % flush_every == 0:
            flush(best)          # sauvegarde de secours du CSV partiel
        if stop:
            print(f"  ✓ cutoff atteint -> arret ({len(best)} types, "
                  f"{pages} pages).", flush=True)
            break
        if not nxt:
            swept = True
            print(f"  ✓ fin des transferts -> BALAYAGE COMPLET "
                  f"({len(best)} types, {pages} pages).", flush=True)
            break
        if plateau_pages and since_new >= plateau_pages:
            print(f"  ✓ couverture plafonnee : {plateau_pages} pages sans "
                  f"nouveau type -> arret ({len(best)} types).", flush=True)
            break
        if deadline is not None and time.monotonic() >= deadline:
            print(f"  ⏱️ budget-temps atteint -> arret PROPRE ({len(best)} "
                  f"types, {pages} pages). Reprise au curseur au prochain "
                  f"dispatch.", flush=True)
            break
        params = dict(nxt)
        time.sleep(cc.PAUSE_BETWEEN_PAGES)
    meta = {"cursor": None if swept else last_nxt, "swept": swept,
            "pages": pages, "oldest": oldest, "types": len(best)}
    return best, meta


if __name__ == "__main__":
    sys.exit(main())
