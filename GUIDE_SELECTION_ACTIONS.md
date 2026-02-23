# Guide Complet — Comment sont sélectionnées vos actions

## Vue d'ensemble

Le système analyse automatiquement environ **1 200 actions** issues de 6 grands indices boursiers :
- S&P 500 (grandes capitalisations américaines)
- Nasdaq 100 (technologie)
- Dow Jones (industriels historiques)
- DAX (Allemagne)
- Euro Stoxx 50 (Europe)
- S&P 600 (petites et moyennes capitalisations américaines)

Ces 1 200 actions passent par **10 étapes d'analyse** successives, sans aucun filtrage prématuré. Ce n'est qu'à la toute fin que les **5 meilleures actions par stratégie** sont sélectionnées.

---

## Les 3 stratégies

Le système produit **3 portefeuilles totalement indépendants**, chacun avec ses propres critères, sa propre logique de sélection et son propre profil de risque.

---

### 1. Court Terme (1 à 30 jours) — « Catalysts »

**Objectif** : capter des mouvements explosifs à très court terme (+75% visé).

**Profil recherché** : des actions volatiles, avec un fort momentum, une pression vendeuse (vente à découvert élevée) et un catalyseur imminent (résultats, annonce, événement).

**Comment les actions sont choisies :**

Chaque action de l'univers reçoit un **Score Court Terme** calculé ainsi :
- **Volume Relatif** (poids : 30%) — on cherche les actions dont le volume de transactions est anormalement élevé par rapport à leur moyenne, signe d'un intérêt soudain du marché (annonce imminente, flux institutionnel)
- **Momentum 1 mois** (poids : 25%) — on mesure la performance sur les 21 derniers jours de bourse, pas sur 1 an (acheter le momentum 1 an = acheter le sommet pour un trade de 30 jours)
- **Vente à découvert** (poids : 25%) — un pourcentage élevé de vente à découvert signifie un potentiel de « short squeeze » (les vendeurs à découvert sont forcés de racheter, ce qui propulse le prix)
- **ATR 14 jours** (poids : 20%) — l'Average True Range mesure la volatilité intraday réelle sur 14 jours, contrairement au VaR qui est une mesure de perte historique maximale et non de mouvement intraday

Les actions sont classées par ce score, et les **5 meilleures** sont retenues.

En parallèle, environ 27 actions supplémentaires sont analysées par l'intelligence artificielle (Perplexity) pour détecter des catalyseurs imminents (résultats trimestriels, partenariats, FDA, etc.). Ces actions rejoignent le même pool.

**Tri final pour choisir les 5 affichées :**
1. Score Court Terme (du plus élevé au plus bas)
2. Score Narratif IA (qualité de l'analyse Perplexity)

---

### 2. Moyen Terme (1 à 8 mois) — « Momentum »

**Objectif** : suivre des tendances établies et fortes (+65% visé).

**Profil recherché** : des actions en tendance claire, soutenues par de l'accumulation institutionnelle, avec un comportement de prix prévisible (pas aléatoire).

**Comment les actions sont choisies :**

On applique des filtres progressifs. On commence par les critères les plus stricts, et on relâche si on ne trouve pas assez de candidats :

**Niveau 1 (le plus strict) :**
- **Exposant de Hurst > 0.52** — cet indicateur mathématique mesure si le prix suit une tendance (> 0.5) ou est aléatoire (= 0.5). On ne garde que les actions en tendance.
- **Prix actuel supérieur à la moyenne mobile 200 jours** — l'action doit être au-dessus de sa tendance longue, signe de force
- **Détention institutionnelle top 10 > 20%** — les 10 plus gros fonds institutionnels doivent détenir au moins 20% du capital, signe d'accumulation professionnelle

**Niveau 2 (si pas assez de candidats) :**
- On relâche le critère institutionnel

**Niveau 3 :**
- On relâche le critère de moyenne mobile et on baisse Hurst à 0.48

**Niveau 4 (dernier recours) :**
- On prend les meilleurs disponibles

Les actions Court Terme déjà sélectionnées sont **exclues** pour éviter les doublons.

Chaque action restante reçoit un **Score Moyen Terme** composite :
- **Exposant de Hurst** (poids : 35%) — force de la tendance
- **Détention institutionnelle** (poids : 30%) — accumulation par les fonds professionnels
- **Performance relative vs S&P 500** (poids : 20%) — surperformance du marché
- **Score de Risque Quantitatif** (poids : 15%) — qualité globale du profil risque

**Tri final pour choisir les 5 affichées :**
1. Score Moyen Terme composite (du plus élevé au plus bas)

---

### 3. Long Terme (1 an et plus) — « Deep Value »

**Objectif** : acheter des actions sous-évaluées et attendre que le marché reconnaisse leur vraie valeur. Le prix cible est la **valeur intrinsèque** calculée mathématiquement.

**Profil recherché** : des actions dont le prix actuel est très en dessous de leur valeur fondamentale réelle, **mais uniquement si l'entreprise est financièrement solide** (pas de « value traps » — entreprises bon marché parce qu'elles vont faire faillite).

**Comment les actions sont choisies :**

On applique des **portes de sécurité strictes** puis des filtres progressifs :

**Niveau 1 (le plus strict) :**
- **Piotroski F-Score ≥ 7** (sur 9) — ce score mesure la solidité du bilan comptable sur 9 critères : rentabilité, liquidité, efficacité opérationnelle. Un score ≥ 7 signifie que l'entreprise est en excellente santé financière.
- **Altman Z-Score ≥ 2.99** — ce score prédit le risque de faillite. Au-dessus de 2.99 = « zone sûre ». Entre 1.81 et 2.99 = « zone grise ». En dessous de 1.81 = « zone de détresse ». **Une action qui ne passe pas ce test est immédiatement rejetée**, peu importe sa marge de sécurité.
- **Marge de Sécurité > 10%** — l'écart entre le prix actuel et la valeur intrinsèque
- **Score de Valeur Profonde > 55**

**Niveau 2 :**
- On relâche à Piotroski ≥ 6, Altman ≥ 2.50, Score Valeur > 40

**Niveau 3 :**
- On relâche à Piotroski ≥ 5, Altman ≥ 1.81 (zone grise acceptée), Score Valeur > 30

**Niveau 4 (dernier recours) :**
- On prend les plus sous-évaluées disponibles sans filtre de qualité

Les actions Court Terme et Moyen Terme déjà sélectionnées sont **exclues**.

**Tri final pour choisir les 5 affichées :**
1. Marge de Sécurité (la plus élevée d'abord — l'action la plus sous-évaluée)
2. Score de Valeur Profonde
3. Score Fondamental

---

## Calcul de la valeur intrinsèque (Long Terme)

La valeur intrinsèque est calculée via une formule simplifiée inspirée de Benjamin Graham :

> Valeur Intrinsèque = Bénéfice par action × (8.5 + 2 × Croissance attendue)

La **marge de sécurité** est ensuite :

> Marge de Sécurité = (Valeur Intrinsèque − Prix Actuel) / Valeur Intrinsèque

Exemple : si la valeur intrinsèque est 100€ et le prix est 8€, la marge de sécurité est de 92%.

---

## Prix cible affiché sur le dashboard

| Stratégie | Formule | Exemple |
|---|---|---|
| **Court Terme** | Prix actuel × 1.75 | Action à 100€ → cible 175€ |
| **Moyen Terme** | Prix actuel × 1.65 | Action à 100€ → cible 165€ |
| **Long Terme** | Prix actuel ÷ (1 − Marge de Sécurité) | Action à 8€, MoS 92% → cible 100€ |

---

## L'analyse par intelligence artificielle (Perplexity)

Pour chaque stratégie, les **5 meilleures actions** sont envoyées à l'IA Perplexity qui analyse en temps réel :
- **Catalyseurs** : pourquoi acheter maintenant (résultats imminents, contrats, FDA, etc.)
- **Menaces** : les risques identifiés (concurrence, dette, insider selling, etc.)
- **Impact IA** : est-ce que l'intelligence artificielle représente une opportunité ou une menace pour cette entreprise

L'IA attribue également un **Score Narratif** de 0 à 100 basé sur le rapport entre signaux positifs et négatifs détectés.

**Total : 15 actions analysées par Perplexity** (5 Court Terme + 5 Moyen Terme + 5 Long Terme), chacune avec ses propres critères. Il n'y a aucun mélange entre les stratégies.

---

## Dimensionnement des positions (Kelly Criterion)

Le système calcule automatiquement quelle **proportion du capital** allouer à chaque action, basé sur le critère de Kelly (utilisé par les hedge funds) :

| Stratégie | Probabilité de gain | Gain moyen | Perte moyenne | Position suggérée |
|---|---|---|---|---|
| **Court Terme** | 55% | +25% | −8% | ~10% du capital |
| **Moyen Terme** | 60% | +50% | −15% | ~12% du capital |
| **Long Terme** | 65% | +100% | −20% | ~15% du capital |

Ces pourcentages sont ajustés individuellement pour chaque action en fonction de son Score Narratif (Court Terme), de son Score de Valeur Profonde (Long Terme) ou de son Score de Risque Quantitatif (Moyen Terme).

---

## Résumé visuel

```
1 200 actions (6 indices mondiaux)
        │
        ▼
   Analyse fondamentale (ROE, croissance, dette, Piotroski, Altman Z)
        │
        ▼
   Valorisation profonde (valeur intrinsèque, marge de sécurité)
        │
        ▼
   Analyse technique (moyennes mobiles, Bollinger, volume, stochastique)
        │
        ▼
   Modèles de risque quantitatif (Hurst, Beta, VaR, Monte Carlo)
        │
        ▼
   Sentiment (FinBERT sur actualités financières)
        │
        ▼
   Analyse événementielle (catalyseurs court terme via IA)
        │
        ▼
   Narratives Perplexity (5 par stratégie, 15 au total)
        │
        ▼
   ┌──────────────────┬──────────────────┬──────────────────┐
   │  5 Court Terme    │  5 Moyen Terme   │  5 Long Terme    │
   │  (explosif)       │  (tendance)      │  (forteresse)    │
   │  RVol+Mom1M+SI    │  Hurst+Instit    │  Piotroski+Alt   │
   │  +ATR             │  +RS+QR          │  +MoS+DeepValue  │
   └──────────────────┴──────────────────┴──────────────────┘
```
