"""
Archive de la publication Medium de VeVe — medium.com/veve-collectibles.

POURQUOI CE MODULE
------------------
`scraper/blog.py` couvre veve.me/blog, qui commence en 2023. Avant ça, VeVe
publiait sur Medium, et s'y est arrêté en avril 2023. Les deux archives sont
COMPLÉMENTAIRES, jamais redondantes :

    medium.com/veve-collectibles   2020 → avril 2023   ← ce module
    veve.me/blog                   2023 → aujourd'hui  ← blog.py

Et ce ne sont pas des billets de blog : ce sont des FICHES TECHNIQUES. Chaque
annonce de drop décrit ses pièces une par une, avec un bloc de champs constant
(Drop Date, List Price, Editions, Rarity, Edition Type, First Edition for Public
Sale, License, Brand, Series). C'est la source PRIMAIRE des tirages et des
raretés — celle qui peut renseigner les 118 objets de catalogue.csv.gz qui n'ont
pas de tirage connu.

CE QUI A ÉTÉ MESURÉ LE 28/07/2026 (et qu'il ne faut pas redécouvrir)
--------------------------------------------------------------------
1. ⛔ medium.com est derrière Cloudflare. `/veve-collectibles/<article>` et
   `/veve-collectibles/sitemap/sitemap.xml` répondent 403 à un client non
   navigateur, TOUJOURS. Seuls `/feed/...` et `/sitemap/...` (global) passent.
   ➡️ On récolte donc depuis la WAYBACK MACHINE : `web/<ts>id_/<url>` rend le
   HTML *brut* d'origine, octet pour octet — le résultat est identique.
2. ⛔ Une passe parallèle sur les sitemaps de Medium (8 puis 16 requêtes) a fait
   tomber TOUT medium.com en 429 pendant ~35 min. Ici : une requête à la fois,
   pause réglable, et arrêt net au premier 429.
3. ⭐ L'ÉNUMÉRATION est le vrai verrou, et elle se résout hors de Medium :
   l'API CDX de la Wayback Machine rend la liste complète en UNE requête.
   472 articles distincts au 28/07/2026.
4. ⚠️ Les sitemaps journaliers de Medium sont PARTIELS : celui du 2020-12-22 en
   liste 7 924 mais pas `adventure-time-series-1`, pourtant publié ce jour-là.
   Le mode `verifier` sert donc à trouver ce que Wayback aurait raté, PAS à
   faire autorité sur le total.
5. ⚠️ Le hash d'URL Medium fait 7 à 12 hexa, pas 12. Un regex `{12}` rate ~40
   articles sur 472.

GARDE-FOUS (regle-collecteurs-longs / regle-instrument-de-mesure)
-----------------------------------------------------------------
- Reprise : le JSONL déjà écrit EST l'état. Un article déjà pris n'est jamais
  refait. On peut relancer le workflow autant de fois qu'on veut.
- Un article n'est compté comme pris QUE si son HTML contient `<article` ET une
  date de publication. Sans ça, une réponse tronquée passerait pour un article
  vide — en silence. C'est exactement l'erreur qui a produit « 1 461 jours
  balayés, 0 résultat » le 28/07.
- 0 article récolté n'écrase JAMAIS le fichier existant.
- Écriture incrémentale : une coupure ne perd que l'article en cours.

⚠️ DROIT D'AUTEUR : ce sont les textes de VeVe. Ce corpus sert à VÉRIFIER et à
SOURCER. Les sites publient les FAITS (dates, tirages, raretés, licences — non
appropriables) avec un lien vers l'annonce, jamais la prose.

VARIABLES D'ENVIRONNEMENT
-------------------------
    MEDIUM_MODE          enumerer | recolter | pieces | verifier | tout  (déf. tout)
    MEDIUM_PUBLICATION   déf. "veve-collectibles"
    MEDIUM_MAX           nb max d'articles à récolter ce run (0 = tout)
    MEDIUM_WORKERS       requêtes Wayback en parallèle (déf. 4, max conseillé 6)
    MEDIUM_PAUSE         pause en s entre deux requêtes Wayback (déf. 0.3)
    MEDIUM_GARDER_HTML   1 = garder le HTML brut gzippé (déf. 0)
    MEDIUM_DEBUT/FIN     bornes AAAA-MM-JJ du mode verifier (déf. 2020-01-01/2023-12-31)
    MEDIUM_SITEMAP_PAUSE pause en s entre deux sitemaps Medium (déf. 1.2)
"""

from __future__ import annotations

import csv
import datetime as _dt
import gzip
import http.client
import json
import os
import re
import sys
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

PUBLICATION = os.environ.get("MEDIUM_PUBLICATION", "veve-collectibles").strip()
RACINE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOSSIER = os.path.join(RACINE, "data")
CORPUS = os.path.join(DOSSIER, "medium_corpus.jsonl")
INDEX = os.path.join(DOSSIER, "medium_index.csv")
PIECES = os.path.join(DOSSIER, "medium_pieces.csv")
ETAT = os.path.join(DOSSIER, "medium_state.json")
HTML_DIR = os.path.join(DOSSIER, "medium_html")

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
ENTETES = {"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"}
TIMEOUT = 90
ESSAIS = 4

WORKERS = max(1, int(os.environ.get("MEDIUM_WORKERS", "2")))
PAUSE = float(os.environ.get("MEDIUM_PAUSE", "2.0"))
MAXI = int(os.environ.get("MEDIUM_MAX", "0"))
GARDER_HTML = os.environ.get("MEDIUM_GARDER_HTML", "").strip() in ("1", "oui", "true")
SITEMAP_PAUSE = float(os.environ.get("MEDIUM_SITEMAP_PAUSE", "1.2"))

_verrou = threading.Lock()

# Un article : medium.com/<publication>/<slug>-<hash 7 à 12 hexa>
RE_ARTICLE = re.compile(
    r"^https?://medium\.com/%s/([^/?#]+?)-([0-9a-f]{7,12})$" % re.escape(PUBLICATION))


def _log(*a):
    print(*a, flush=True)


# ── LE RÉSEAU, ET LA LEÇON DU 1er RUN GITHUB (28/07/2026) ───────────────────
# Sur un runner GitHub, archive.org a refusé la connexion (Errno 111) sur
# TOUTES les requêtes d'article — alors que l'énumération, elle, passait.
# La différence entre les deux : l'énumération appelait http://, la récolte
# https://. archive.org étrangle les IP de datacenter, et ça se manifeste par un
# refus TCP sec, pas par un code HTTP.
#
# Trois réponses, dans cet ordre :
#   1. UNE CONNEXION RÉUTILISÉE par thread (keep-alive). urllib rouvrait un
#      socket + une poignée de main TLS par article : 472 ouvertures, c'est
#      exactement ce que le pare-feu compte.
#   2. REPLI AUTOMATIQUE https → http quand la connexion est refusée.
#   3. ATTENTE LONGUE (60 s, 120 s…) sur un refus, pas 3 s : un refus n'est pas
#      un incident réseau, c'est un ordre de ralentir.
_tls = threading.local()
_repli_http = threading.Event()      # une fois vrai, tout le monde passe en http


def _connexion(scheme: str, hote: str):
    """Une connexion persistante par thread. La rouvrir coûte plus que l'octet."""
    cle = (scheme, hote)
    c = getattr(_tls, "conn", None)
    if c is not None and getattr(_tls, "cle", None) == cle:
        return c
    if c is not None:
        try:
            c.close()
        except Exception:                            # noqa: BLE001
            pass
    fabrique = (http.client.HTTPSConnection if scheme == "https"
                else http.client.HTTPConnection)
    c = fabrique(hote, timeout=TIMEOUT)
    _tls.conn, _tls.cle = c, cle
    return c


def _oublier_connexion():
    c = getattr(_tls, "conn", None)
    if c is not None:
        try:
            c.close()
        except Exception:                            # noqa: BLE001
            pass
    _tls.conn = _tls.cle = None


def _decompresser(corps: bytes, encodage: str | None) -> bytes:
    """⭐⭐ LE PIÈGE `id_` (mesuré le 28/07, 6 articles perdus dessus).

    `web/<ts>id_/<url>` rejoue la réponse d'ORIGINE **telle quelle**, en-têtes
    compris. Si Medium l'avait servie en gzip, on reçoit du gzip — même sans
    avoir demandé d'`Accept-Encoding`. Sur 472 articles, 6 étaient dans ce cas :
    le décodage donnait des octets binaires, donc ni `<article>` ni date, donc
    la sonde les rejetait « proprement ». Un article parfaitement archivé
    comptait comme perdu.

    ⚠️ On renifle les octets magiques EN PLUS de l'en-tête : l'en-tête peut
    manquer sur une vieille capture.
    """
    if not corps:
        return corps
    enc = (encodage or "").lower()
    try:
        if corps[:2] == b"\x1f\x8b" or "gzip" in enc:
            return gzip.decompress(corps)
        if "deflate" in enc:
            import zlib
            try:
                return zlib.decompress(corps)
            except zlib.error:
                return zlib.decompress(corps, -zlib.MAX_WBITS)
    except Exception:                                # noqa: BLE001
        return corps            # illisible : on rend le brut, la sonde tranchera
    return corps


def _requete(url: str):
    """GET keep-alive. Rend (code, corps). Lève si la connexion est impossible."""
    p = urllib.parse.urlsplit(url)
    scheme = "http" if (_repli_http.is_set() and p.netloc.endswith("archive.org")) \
        else p.scheme
    chemin = p.path + (("?" + p.query) if p.query else "")
    dernier = None
    for tentative in (1, 2):     # une connexion gardée trop longtemps se ferme
        c = _connexion(scheme, p.netloc)
        try:
            c.request("GET", chemin, headers=dict(ENTETES, Connection="keep-alive"))
            r = c.getresponse()
            corps = r.read()
            if r.status in (301, 302, 303, 307, 308):
                cible = r.getheader("Location")
                if cible:
                    return _requete(urllib.parse.urljoin(url, cible))
            return r.status, _decompresser(corps, r.getheader("Content-Encoding"))
        except Exception as e:                       # noqa: BLE001
            dernier = e
            _oublier_connexion()
    raise dernier


def _get(url: str, essais: int = ESSAIS, silencieux: bool = False) -> bytes | None:
    """GET avec backoff. Rend None plutôt que de lever : la récolte continue."""
    dernier = ""
    for n in range(essais):
        try:
            code, corps = _requete(url)
            if code == 200:
                return corps
            dernier = "HTTP %d" % code
            time.sleep((60 if code == 429 else 5) * (n + 1))
        except Exception as e:                       # noqa: BLE001
            dernier = str(e)
            refus = ("Connection refused" in dernier or "handshake" in dernier
                     or "timed out" in dernier)
            if refus and url.startswith("https://") and not _repli_http.is_set():
                _repli_http.set()
                _log("   ↩️ connexion refusée en https → bascule de tout le run "
                     "en http (c'est ce qui faisait passer l'énumération).")
                continue                              # on retente tout de suite
            time.sleep((60 * (n + 1)) if refus else (5 * (n + 1)))
    if not silencieux:
        _log("      ⚠️ échec après %d essais : %s (%s)" % (essais, url, dernier))
    return None


# ─────────────────────────── 1. ÉNUMÉRATION ────────────────────────────────
def enumerer() -> list[dict]:
    """Liste datée des articles, via l'API CDX de la Wayback Machine.

    UNE requête, et zéro charge sur medium.com. `collapse=urlkey` donne un
    snapshot par URL ; on garde le plus ANCIEN, le plus proche de la publication.
    """
    url = ("http://web.archive.org/cdx/search/cdx?"
           + urllib.parse.urlencode({
               "url": "medium.com/%s*" % PUBLICATION,
               "output": "json",
               "fl": "original,timestamp,statuscode",
               "collapse": "urlkey",
               "filter": "statuscode:200",
           }))
    brut = _get(url, essais=5)
    if not brut:
        _log("⛔ CDX injoignable — on ne touche à rien.")
        return []
    lignes = json.loads(brut.decode("utf-8", "replace"))[1:]
    meilleur: dict = {}
    for orig, ts, _sc in lignes:
        u = orig.split("?")[0].split("#")[0].rstrip("/")
        m = RE_ARTICLE.match(u)
        if not m:
            continue
        h = m.group(2)
        if h not in meilleur or ts < meilleur[h][1]:
            meilleur[h] = (u, ts, m.group(1))
    taches = [{"hash": h, "url": u, "ts": ts, "slug": s}
              for h, (u, ts, s) in sorted(meilleur.items())]
    _log("📚 énumération : %d articles distincts (%d lignes CDX)"
         % (len(taches), len(lignes)))
    return taches


# ─────────────────────────── 2. RÉCOLTE ────────────────────────────────────
def _meta(h: str, prop: str) -> str | None:
    for motif in (r'<meta[^>]+(?:property|name)="%s"[^>]+content="([^"]*)"',
                  r'<meta[^>]+content="([^"]*)"[^>]+(?:property|name)="%s"'):
        m = re.search(motif % re.escape(prop), h)
        if m:
            # ⚠️ Les <meta> de Medium sont echappees : sans ca, un titre sort
            # « Batman Black &amp; White » et se retrouve tel quel dans le CSV.
            return _desechapper(m.group(1))
    return None


def _metas(h: str, prop: str) -> list:
    return re.findall(
        r'<meta[^>]+(?:property|name)="%s"[^>]+content="([^"]*)"' % re.escape(prop), h)


_ENTITES = {"&nbsp;": " ", "&amp;": "&", "&lt;": "<", "&gt;": ">", "&quot;": '"',
            "&#39;": "'", "&hellip;": "...", "&mdash;": "—", "&ndash;": "–",
            "&rsquo;": "’", "&lsquo;": "‘", "&ldquo;": "“", "&rdquo;": "”"}


def _desechapper(s: str) -> str:
    for a, b in _ENTITES.items():
        s = s.replace(a, b)
    s = re.sub(r"&#x([0-9a-fA-F]+);", lambda m: chr(int(m.group(1), 16)), s)
    s = re.sub(r"&#(\d+);", lambda m: chr(int(m.group(1))), s)
    # Medium encadre ses tirets d'espaces fines (U+200A) : invisible a l'oeil,
    # mais ca casse toute jointure par le titre. On normalise.
    return s.replace(" ", " ").replace(" ", " ").replace("\xa0", " ")


def html_vers_texte(h: str) -> str:
    """Le corps de l'article, balises ôtées, structure gardée par des sauts de ligne.

    ⚠️ Les sauts de ligne ne sont PAS cosmétiques : le parseur de pièces s'appuie
    dessus pour séparer « Drop Date: … » de « List Price: … », que Medium écrit
    dans un seul <p> avec des <br>.
    """
    m = re.search(r"(?s)<article\b.*?</article>", h)
    s = m.group(0) if m else h
    s = re.sub(r"(?s)<(script|style|noscript)\b.*?</\1>", " ", s)
    s = re.sub(r"(?i)<br\s*/?>", "\n", s)
    s = re.sub(r"(?i)</(p|div|h[1-6]|li|figure|figcaption|blockquote|tr)>", "\n", s)
    s = re.sub(r"<[^>]+>", "", s)
    s = _desechapper(s)
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n[ \t]+", "\n", s)
    return re.sub(r"\n{3,}", "\n\n", s).strip()


def _lire_corpus() -> dict:
    """Le JSONL déjà écrit EST l'état de reprise. Pas de fichier de marqueurs."""
    fiches: dict = {}
    if not os.path.exists(CORPUS):
        return fiches
    with open(CORPUS, encoding="utf-8") as f:
        for ligne in f:
            ligne = ligne.strip()
            if not ligne:
                continue
            try:
                d = json.loads(ligne)
            except json.JSONDecodeError:
                continue                     # ligne coupée par un kill : on l'oublie
            if d.get("hash"):
                fiches[d["hash"]] = d
    return fiches


_compteur = {"ok": 0, "echec": 0}


def _autres_horodatages(url: str, sauf: str) -> list:
    """Les autres captures de cette URL. ⭐ Le 1er run a perdu 20 articles sur des
    404/410 : l'instantané choisi était mort, pas l'article. Une capture ratée
    n'est pas un article manquant — il suffit d'en demander une autre."""
    q = ("http://web.archive.org/cdx/search/cdx?"
         + urllib.parse.urlencode({"url": url, "output": "json",
                                   "fl": "timestamp", "filter": "statuscode:200",
                                   "limit": "12"}))
    brut = _get(q, essais=2, silencieux=True)
    if not brut:
        return []
    try:
        return [l[0] for l in json.loads(brut.decode("utf-8", "replace"))[1:]
                if l[0] != sauf]
    except Exception:                                # noqa: BLE001
        return []


def _prendre(t: dict) -> str:
    horodatages = [t["ts"]]
    for n in range(2):                # 2 passes ; le backoff vit dans _get
        if n and len(horodatages) == 1:
            horodatages += _autres_horodatages(t["url"], t["ts"])[:3]
        brut = None
        for ts in horodatages[(1 if n else 0):] or horodatages:
            brut = _get("https://web.archive.org/web/%sid_/%s" % (ts, t["url"]),
                        essais=(ESSAIS if n == 0 else 1), silencieux=(n == 0))
            if brut is not None:
                break
        if brut is None:
            continue
        h = brut.decode("utf-8", "replace")
        date = _meta(h, "article:published_time")
        # ⭐ LA sonde qui manquait le 28/07 : sans <article> ni date, ce n'est pas
        #    un article vide, c'est une réponse inutilisable. On NE l'écrit PAS.
        if "<article" not in h or not date:
            time.sleep(5 * (n + 1))
            continue
        if GARDER_HTML:
            os.makedirs(HTML_DIR, exist_ok=True)
            with gzip.open(os.path.join(HTML_DIR, t["hash"] + ".html.gz"),
                           "wt", encoding="utf-8") as f:
                f.write(h)
        texte = html_vers_texte(h)
        fiche = {
            "hash": t["hash"], "url": t["url"], "slug": t["slug"],
            "date_publication": date,
            "date_maj": _meta(h, "article:modified_time"),
            "titre": _meta(h, "og:title"),
            "resume": _meta(h, "og:description"),
            "auteur": _meta(h, "author") or _meta(h, "article:author"),
            "tags": _metas(h, "article:tag"),
            "image": _meta(h, "og:image"),
            "wayback_ts": t["ts"],
            "octets_html": len(h),
            "caracteres_texte": len(texte),
            "texte": texte,
        }
        with _verrou:
            with open(CORPUS, "a", encoding="utf-8") as f:
                f.write(json.dumps(fiche, ensure_ascii=False) + "\n")
            _compteur["ok"] += 1
            n_ok = _compteur["ok"]
        # Un point d'avancement régulier : un log de 6 h sans repère, on ne sait
        # pas s'il avance ou s'il tourne à vide.
        if n_ok % 25 == 0:
            _log("   … %d articles pris (%s)" % (n_ok, (fiche["date_publication"] or "")[:10]))
        time.sleep(PAUSE)
        return "ok"
    with _verrou:
        _compteur["echec"] += 1
    return "echec"


def recolter(taches: list) -> dict:
    deja = _lire_corpus()
    restant = [t for t in taches if t["hash"] not in deja]
    if MAXI:
        restant = restant[:MAXI]
    _log("🌾 récolte : %d déjà en base, %d à prendre ce run (%d workers, pause %.2fs)"
         % (len(deja), len(restant), WORKERS, PAUSE))
    if not restant:
        return {"pris": 0, "echecs": 0, "total": len(deja)}

    # ⭐ SONDE D'ENTRÉE. Le 1er run GitHub a défilé 472 échecs identiques avant
    # d'abandonner : le log était illisible et le diagnostic noyé. On teste UN
    # article ; s'il ne passe pas, on le dit en une ligne et on s'arrête. Un
    # collecteur qui n'arrive pas à collecter doit le dire, pas insister 6 h.
    if _prendre(restant[0]) != "ok":
        _log("⛔ ARRÊT : archive.org n'a pas répondu sur un article témoin, "
             "même après repli http et attentes longues.\n"
             "   Ce n'est pas un problème de code : ce runner est bloqué côté "
             "archive.org.\n"
             "   → réessayer plus tard, ou baisser à MEDIUM_WORKERS=1 et "
             "MEDIUM_PAUSE=8.")
        return {"pris": 0, "echecs": 1, "total": len(deja), "bloque": True}
    restant = restant[1:]
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        list(ex.map(_prendre, restant))
    total = len(_lire_corpus())
    _log("   → %d pris, %d échecs, %d en base, %.0f s%s"
         % (_compteur["ok"], _compteur["echec"], total, time.time() - t0,
            "  (en http)" if _repli_http.is_set() else ""))
    return {"pris": _compteur["ok"], "echecs": _compteur["echec"], "total": total,
            "repli_http": _repli_http.is_set()}


# ─────────────────────────── 3. TABLE DES PIÈCES ───────────────────────────
# Une ligne « Champ: valeur » dans le corps. On ne code pas en dur la liste des
# champs : VeVe en a changé au fil des ans (les comics ont « Cover Variants »,
# « Total Editions », « Published » que les figurines n'ont pas). On ramasse tout
# et on range les champs canoniques dans des colonnes fixes ; le reste part dans
# une colonne « autres » en JSON. ⭐ Un champ inconnu n'est jamais perdu.
RE_CHAMP = re.compile(r"^([A-Z][A-Za-z0-9 ’'&/#.\-—–\(\)]{1,45}):[ \t]*(.*)$")

# ⭐⭐ LE TIRAGE PAR COUVERTURE. Pour un comic, VeVe ne donne PAS un tirage mais
# un par variante :
#     Total Editions: 20,000
#     COMMON — Classic Cover: 12,000
#     UNCOMMON — Vintage Variant: 4,500
# Or le catalogue, lui, a UNE LIGNE PAR VARIANTE (« Immortal Hulk #1 (Classic
# Cover) », COMMON). Sans ce découpage, l'annonce et le catalogue ne se joignent
# jamais — et c'est le tiret cadratin qui faisait tout rater : il n'était pas
# dans les caractères admis d'un nom de champ, donc ces lignes n'existaient pas.
RE_VARIANTE = re.compile(
    r"^(COMMON|UNCOMMON|RARE|ULTRA[ _-]?RARE|SECRET[ _-]?RARE|LEGENDARY|EXCLUSIVE)"
    r"\s*[—–-]\s*(.+)$", re.I)

# ⭐ Les ALIAS ne sont pas du confort : en 2020 VeVe ecrivait « Price » et
# « Type », il n'est passe a « List Price » / « Edition Type » qu'ensuite. Sans
# eux, les annonces les plus anciennes — justement celles qu'aucune autre source
# ne documente — sortent avec leurs colonnes vides.
CANONIQUES = {
    "drop date": "drop_date",
    "list price": "list_price",
    "price": "list_price",
    "editions": "editions",
    "artist": "artist",
    "type": "edition_type",
    "total editions": "total_editions",
    "rarity": "rarity",
    "edition type": "edition_type",
    "first edition for public sale": "first_edition_public_sale",
    "license": "license",
    "brand": "brand",
    "series": "series",
    "cover variants": "cover_variants",
    "published": "published",
    "available": "available",
}
COLONNES = ["url", "date_publication", "titre_article", "piece", "drop_date",
            "list_price", "editions", "total_editions", "rarity", "edition_type",
            "first_edition_public_sale", "license", "brand", "series", "artist",
            "cover_variants", "published", "available", "variante", "autres"]

# Le chrome de Medium, qu'il ne faut JAMAIS prendre pour un nom de pièce.
# ⚠️ La ligne « Dec 17, 2020 · 3 min read » est le piège : elle précède le
# premier bloc de champs et se faisait passer pour le nom de la 1re pièce.
# ⚠️ « VeVeFollow » aussi : le nom de l'auteur et son bouton, collés sans espace.
BRUIT = re.compile(
    r"^(follow|share|listen|·|\d+ min read|sign in|sign up|open in app|"
    r"veve ?follow|veve digital collectibles|published in|written by)\b"
    r"|min read|^[A-Z][a-z]{2} \d{1,2},? \d{4}\s*$", re.I)

# Une phrase n'est pas un nom de pièce. Les noms VeVe (« Savage She-Hulk #1 »,
# « Adventure Time — Series 1 ») ne se terminent jamais par une ponctuation de
# phrase ; les accroches marketing, si — et elles se retrouvaient en nom.
RE_PHRASE = re.compile(r"[.!?]\s*$")


def _pieces_d_un_texte(texte: str) -> list:
    """Chaque suite contiguë de « Champ: valeur » est une pièce.

    Son NOM est la dernière ligne non vide qui précède le bloc et qui n'est pas
    elle-même un champ — c'est ainsi que VeVe met en page ses annonces.
    """
    lignes = [l.strip() for l in texte.split("\n")]
    sorties, courant, nom, dernier_libre = [], {}, None, None

    def vider():
        nonlocal courant, nom
        if courant:
            sorties.append({"piece": nom or "", "champs": courant})
        courant, nom = {}, None

    for l in lignes:
        m = RE_CHAMP.match(l) if l else None
        if m:
            cle, val = m.group(1).strip(), m.group(2).strip()
            if not courant:
                nom = dernier_libre
            elif cle in courant:        # même champ deux fois = nouvelle pièce
                vider()
                nom = dernier_libre
            courant[cle] = val
        else:
            if courant:
                vider()
            # ⚠️ search, PAS match : « Dec 17, 2020 · 3 min read » ne commence
            # par aucun des mots-clés, c'est « min read » au milieu qui le trahit.
            if l and not BRUIT.search(l) and not RE_PHRASE.search(l) and len(l) < 90:
                dernier_libre = l
    vider()
    # Un bloc d'un seul champ, c'est du bruit (« Note: … »), pas une pièce.
    return [s for s in sorties if len(s["champs"]) >= 2]


def construire_pieces() -> int:
    fiches = list(_lire_corpus().values())
    fiches.sort(key=lambda d: d.get("date_publication") or "")
    lignes = []
    for f in fiches:
        for p in _pieces_d_un_texte(f.get("texte") or ""):
            row = {c: "" for c in COLONNES}
            row["url"] = f["url"]
            row["date_publication"] = (f.get("date_publication") or "")[:10]
            # Renormalise ici aussi : une fiche récoltée par une version
            # antérieure du module garde son titre échappé.
            row["titre_article"] = _desechapper(f.get("titre") or "")
            row["piece"] = p["piece"]
            autres = {}
            for cle, val in p["champs"].items():
                col = CANONIQUES.get(cle.lower())
                if col:
                    row[col] = val
                else:
                    autres[cle] = val
            # ⭐ Une ligne de plus PAR VARIANTE de couverture, nommée comme le
            # catalogue la nomme : « Immortal Hulk #1 (Classic Cover) ». La ligne
            # d'annonce reste, elle : elle porte le tirage TOTAL, qu'aucune ligne
            # de variante ne dit.
            variantes = []
            for cle, val in list(autres.items()):
                m = RE_VARIANTE.match(cle)
                if not m or not re.fullmatch(r"[\d.,\s]+", val or ""):
                    continue
                rarete = re.sub(r"[ -]+", "_", m.group(1).strip().upper())
                nom_var = m.group(2).strip()
                fils = dict(row)
                fils["piece"] = "%s (%s)" % (row["piece"], nom_var) if row["piece"] else nom_var
                fils["variante"] = nom_var
                fils["rarity"] = rarete
                fils["editions"] = val.strip()
                fils["total_editions"] = ""
                fils["autres"] = ""
                variantes.append(fils)
                autres.pop(cle, None)
            row["autres"] = json.dumps(autres, ensure_ascii=False) if autres else ""
            lignes.append(row)
            lignes.extend(variantes)
    if not lignes:
        _log("⚠️ 0 pièce extraite — on n'écrase PAS %s" % PIECES)
        return 0
    os.makedirs(DOSSIER, exist_ok=True)
    with open(PIECES, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLONNES)
        w.writeheader()
        w.writerows(lignes)
    avec_tirage = sum(1 for r in lignes if r["editions"] or r["total_editions"])
    _log("🧾 table des pièces : %d lignes, %d avec un tirage → %s"
         % (len(lignes), avec_tirage, os.path.basename(PIECES)))
    return len(lignes)


def construire_index() -> int:
    fiches = list(_lire_corpus().values())
    if not fiches:
        _log("⚠️ corpus vide — on n'écrase PAS %s" % INDEX)
        return 0
    fiches.sort(key=lambda d: d.get("date_publication") or "")
    with open(INDEX, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "titre", "url", "caracteres", "auteur"])
        for d in fiches:
            w.writerow([(d.get("date_publication") or "")[:10],
                        _desechapper(d.get("titre") or ""),
                        d["url"], d.get("caracteres_texte") or 0, d.get("auteur") or ""])
    _log("🗂️ index : %d articles → %s" % (len(fiches), os.path.basename(INDEX)))
    return len(fiches)


# ─────────────── 4. VÉRIFICATION DE COMPLÉTUDE (sitemaps Medium) ────────────
def verifier(taches: list) -> dict:
    """Balaye les sitemaps journaliers de Medium pour trouver ce que Wayback a raté.

    ⚠️ Ces sitemaps sont PARTIELS (mesuré) : ils ne font PAS autorité sur le
    total. Ils ne servent qu'à ça : révéler une URL absente de la liste Wayback.
    ⛔ SÉQUENTIEL, une requête à la fois. Une passe parallèle a fait tomber tout
    medium.com en 429 pendant 35 min le 28/07/2026.
    """
    debut = os.environ.get("MEDIUM_DEBUT", "2020-01-01")
    fin = os.environ.get("MEDIUM_FIN", "2023-12-31")
    d = _dt.date.fromisoformat(debut)
    dfin = _dt.date.fromisoformat(fin)
    # ⚠️ NE PAS COMPARER DES URL BRUTES. Medium écrit « citroën » en clair dans
    # son sitemap, la Wayback Machine « citro%C3%ABn ». Sans cette normalisation,
    # le contrôle annonce un article manquant qui est en base depuis le début —
    # une fausse alerte due à l'instrument, pas à la donnée.
    def _cle(u):
        return urllib.parse.unquote(u).rstrip("/")

    connus = {_cle(t["url"]) for t in taches}
    trouves: dict = {}
    jours = 0
    bride = False
    _log("🔎 vérification via les sitemaps Medium, %s → %s (séquentiel, pause %.1fs)"
         % (debut, fin, SITEMAP_PAUSE))
    while d <= dfin:
        u = "https://medium.com/sitemap/posts/%d/posts-%s.xml" % (d.year, d.isoformat())
        try:
            with urllib.request.urlopen(
                    urllib.request.Request(u, headers=ENTETES), timeout=TIMEOUT) as r:
                x = r.read().decode("utf-8", "replace")
        except Exception as e:                       # noqa: BLE001
            if getattr(e, "code", None) == 429:
                bride = True
                _log("   ⛔ 429 au %s — Medium nous bride. Arrêt de la vérification "
                     "(%d jours faits). La récolte, elle, n'en dépend pas." % (d, jours))
                break
            d += _dt.timedelta(days=1)
            continue
        # ⭐ La sonde : sans </urlset>, la réponse est tronquée ou piégée.
        if "</urlset>" not in x[-400:]:
            d += _dt.timedelta(days=1)
            continue
        jours += 1
        for loc in re.findall(r"<loc>(https://medium\.com/%s/[^<]+)</loc>"
                              % re.escape(PUBLICATION), x):
            if _cle(loc) not in connus:
                trouves[loc] = d.isoformat()
        d += _dt.timedelta(days=1)
        time.sleep(SITEMAP_PAUSE)
    if trouves:
        _log("   ⭐ %d URL vues par Medium et ABSENTES de la liste Wayback :"
             % len(trouves))
        for u, j in sorted(trouves.items(), key=lambda kv: kv[1]):
            _log("      %s  %s" % (j, u))
    elif bride:
        # ⚠️ Ne JAMAIS dire « rien ne manque » quand on s'est fait couper : ce
        # serait annoncer un résultat alors qu'on a arrêté de mesurer.
        _log("   ⚠️ %d jours seulement ont été lus avant le bridage — cette passe "
             "ne conclut RIEN sur les %d autres." % (jours, (dfin - d).days + 1))
    else:
        _log("   ✅ aucune URL manquante sur les %d jours lus." % jours)
    return {"jours_lus": jours, "manquantes": trouves, "bride": bride}


# ─────────────────────────────── PILOTE ────────────────────────────────────
def main() -> int:
    mode = os.environ.get("MEDIUM_MODE", "tout").strip().lower()
    os.makedirs(DOSSIER, exist_ok=True)
    _log("═══ Archive Medium · %s · mode=%s ═══" % (PUBLICATION, mode))

    # ⭐ `pieces` ne touche PAS au réseau : c'est un mode de reparse, il doit
    # marcher hors ligne et en 2 s. Le lui faire appeler l'énumération, c'est le
    # rendre otage d'une API tierce pour relire des fichiers déjà sur le disque.
    taches: list = []
    if mode != "pieces":
        taches = enumerer()
        if not taches:
            return 1
    etat = {
        "publication": PUBLICATION,
        "horodatage": _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds"),
        "articles_enumeres": len(taches),
    }

    if mode in ("recolter", "tout"):
        etat["recolte"] = recolter(taches)
    if mode in ("pieces", "recolter", "tout"):
        etat["articles_en_base"] = construire_index()
        etat["pieces"] = construire_pieces()
    if mode in ("verifier", "tout"):
        v = verifier(taches)
        etat["verification"] = {"jours_lus": v["jours_lus"],
                                "manquantes": sorted(v["manquantes"]),
                                "bride_par_medium": v["bride"]}

    with open(ETAT, "w", encoding="utf-8") as f:
        json.dump(etat, f, ensure_ascii=False, indent=2)
    en_base = etat.get("articles_en_base") or len(_lire_corpus())
    _log("═══ fini · %d articles en base%s ═══"
         % (en_base, (" sur %d énumérés" % len(taches)) if taches else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
