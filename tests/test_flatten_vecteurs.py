# -*- coding: utf-8 -*-
# ⚠️ CE FICHIER EST PARTAGE PAR TROIS DEPOTS — c'est tout son interet.
#   VeVePreda/scrapeur-veve          tests/test_flatten_vecteurs.py   (REFERENCE)
#   astronemagame-maker/astronema    tests/test_flatten_vecteurs.py
#   lepaolo/paolo                    tests/test_flatten_vecteurs.py
# Avec `tests/vecteurs_flatten.json`, a copier tel quel a cote.

"""🧬 LES TROIS COPIES DE `_flatten` DOIVENT REPONDRE PAREIL — phase 4.1.

`scraper/collectchain.py` existe en trois exemplaires : 615 lignes ici, 533
dans `astronema`, 533 dans `paolo`. Une seule est instrumentee par la
sentinelle, et une seule a recu le lot 63.

⭐⭐ DEUX COPIES D'UN COLLECTEUR NE DIVERGENT PAS LE JOUR OU ON LES EDITE, MAIS
LE JOUR OU UNE SEULE EST CORRIGEE. Les 216 838 transferts mal etiquetes (26,9 %)
sont exactement ce scenario.

## ⛔ POURQUOI DES VECTEURS ET PAS UNE COMPARAISON DE CODE

Un banc ne peut pas lire les fichiers d'un autre depot : il n'y a pas de
checkout croise, et il n'y en aura pas. Comparer les SOURCES etait donc
impossible — mais c'etait aussi la mauvaise question.

⭐⭐⭐ DEUX IMPLEMENTATIONS QUI DIFFERENT NE SONT PAS UN PROBLEME ; DEUX
RESULTATS QUI DIFFERENT EN SONT UN. Un diff de code aurait rougi sur un
commentaire reformule et se serait tu sur un `if` inverse.

➡️ Ce fichier fige donc le COMPORTEMENT : des transferts bruts au format exact
de l'API CollectScan, et ce que `_flatten` doit en produire. Chaque depot
embarque les memes vecteurs et les joue sur SA copie. Aucun acces croise, et la
divergence se nomme.

## 📌 SI CE BANC ROUGIT DANS `astronema` OU `paolo`

Ce n'est pas le banc qui est faux : c'est cette copie de `collectchain.py` qui
est en retard sur la reference (`VeVePreda/scrapeur-veve`). Le message nomme le
champ. ⛔ Ne pas regenerer les vecteurs depuis la copie en retard — ce serait
graver la divergence au lieu de la corriger.

## 🔄 REGENERER LES VECTEURS (depuis la REFERENCE uniquement)

Quand VeVe ajoute un champ et que `_flatten` de `scrapeur-veve` evolue :
relancer le generateur (cf. `A-LIRE-lot65.md`), relire le diff du JSON A LA
MAIN, puis re-copier le JSON dans les trois depots.
"""
from __future__ import annotations

import json
import os
import pathlib
import sys

import pytest

RACINE = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RACINE))
from scraper import collectchain as cc                     # noqa: E402

VECTEURS = pathlib.Path(__file__).resolve().parent / "vecteurs_flatten.json"

# Champs dont la valeur depend de l'ENVIRONNEMENT et pas de la logique : ils
# sont exclus de la comparaison, avec leur raison. ⛔ Pas de set muet.
HORS_COMPARAISON = {
    "ts": "objet datetime — sa representation depend de la version de Python",
}


def _cas():
    """⛔ RETOURNE UNE LISTE VIDE, NE `skip` PAS. Un `pytest.skip` appele au
    moment du `parametrize` casse la COLLECTE : message illisible, et surtout
    un banc qui se desactive tout seul quand ses donnees manquent.
    ⭐⭐⭐ UN BANC QUI SE SKIPPE FAUTE DE DONNEES EST UN BANC VERT QUI NE TESTE
    RIEN — le pire des trois etats possibles, parce qu'il ressemble a un
    succes. Ici, l'absence du JSON fait ECHOUER
    `test_les_vecteurs_sont_bien_la_et_couvrent_les_kind`, avec le chemin exact
    du fichier a copier."""
    if not VECTEURS.exists():
        return []
    return json.loads(VECTEURS.read_text(encoding="utf-8"))["cas"]


def test_les_vecteurs_sont_bien_la_et_couvrent_les_kind():
    """⭐⭐ UN BANC QUI NE TROUVE PAS SES DONNEES PASSE AU VERT. Le `skip`
    ci-dessus est necessaire (un depot peut ne pas encore avoir copie le JSON)
    et dangereux : ce test verifie qu'une fois present, il est COMPLET."""
    cas = _cas()
    assert cas, (
        f"⛔ {VECTEURS} est ABSENT. Ce banc compare le comportement de la copie "
        f"locale de `collectchain._flatten` a celui de la reference "
        f"(VeVePreda/scrapeur-veve) : sans les vecteurs, il ne teste rien. "
        f"Copier `tests/vecteurs_flatten.json` depuis le depot de reference.")
    assert len(cas) >= 10, f"seulement {len(cas)} vecteurs"
    kinds = {c["attendu"]["kind"] for c in cas}
    manquants = {"mint", "vault_mint", "burn", "listing",
                 "system_transfer", "market"} - kinds
    assert not manquants, f"aucun vecteur pour : {sorted(manquants)}"


@pytest.mark.parametrize("cas", _cas(), ids=lambda c: c["nom"][:48])
def test_flatten_repond_comme_la_reference(cas):
    """LE TEST CENTRAL, un cas par ligne pour que l'echec NOMME le cas."""
    obtenu = cc._flatten(cas["brut"])
    assert obtenu is not None, f"`_flatten` a rendu None sur « {cas['nom']} »"

    attendu = {k: v for k, v in cas["attendu"].items()
               if k not in HORS_COMPARAISON}

    manquants = sorted(set(attendu) - set(obtenu))
    assert not manquants, (
        f"« {cas['nom']} » : champ(s) que cette copie ne produit PAS : "
        f"{manquants}. Cette copie de `collectchain.py` est en retard sur la "
        f"reference (VeVePreda/scrapeur-veve).")

    for champ, valeur in sorted(attendu.items()):
        assert obtenu[champ] == valeur, (
            f"« {cas['nom']} » · champ `{champ}` :\n"
            f"    reference = {valeur!r}\n"
            f"    obtenu    = {obtenu[champ]!r}")


def test_les_champs_EN_TROP_sont_signales_sans_faire_echouer(request):
    """⭐ UN CHAMP EN PLUS N'EST PAS UNE DIVERGENCE — c'est peut-etre la
    reference qui est en retard sur une copie, ou un champ neuf pas encore
    verse dans les vecteurs. On le NOMME sans bloquer : bloquer ici rendrait
    impossible d'ajouter un champ sans regenerer d'abord les vecteurs, et cette
    friction finirait par faire supprimer le banc."""
    extras = set()
    for cas in _cas():
        obtenu = cc._flatten(cas["brut"]) or {}
        extras |= set(obtenu) - set(cas["attendu"])
    if extras:
        print(f"\nℹ️ champ(s) produits par cette copie et absents des vecteurs : "
              f"{sorted(extras)} — regenerer les vecteurs DEPUIS LA REFERENCE "
              f"si c'est voulu.")


def test_le_MINT_anonyme_garde_son_token_id():
    """⭐⭐⭐ LE CAS QUI PORTE TOUTE LA PHASE 2, isole pour qu'il ne se perde pas
    dans la liste. 167 159 transferts CollectChain n'ont pas de `veve_uuid`
    (99,96 % sont des mints : au mint la metadonnee n'est pas encore attachee,
    donc pas d'image, donc pas d'uuid). `total.token_id` est un champ du
    TRANSFERT, pas de la metadonnee : il survit la ou l'uuid meurt.
    ⭐⭐ UNE LIGNE ANONYME EST UNE LIGNE DONT ON A LU LE MAUVAIS CHAMP."""
    cas = [c for c in _cas() if c["attendu"]["kind"] in ("mint", "vault_mint")
           and not c["attendu"]["veve_uuid"]]
    assert cas, "aucun vecteur de mint anonyme — le cas central n'est pas couvert"
    for c in cas:
        r = cc._flatten(c["brut"])
        assert r["veve_uuid"] == ""
        assert r["token_id"], (
            f"« {c['nom']} » : token_id vide sur un mint. Cette copie lit "
            f"`token_id` dans la metadonnee au lieu de `total` — c'est "
            f"exactement l'erreur que ce banc existe pour attraper.")
