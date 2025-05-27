# Schéma d'architeture 

```bash
┌─────────────┐     ┌───────────────┐     ┌─────────────┐     ┌─────────────┐
│             │     │   Workers     │     │             │     │             │
│ Orchestrator├────►│ (Map Phase)   ├────►│ Aggregator  ├────►│    API      │
│             │     │ ┌────┐ ┌────┐ │     │ (Reduce)    │     │             │
└──────┬──────┘     │ │W1  │ │W2..│ │     └─────────────┘     └─────────────┘
       │            └─┴────┴─┴────┴─┘             ▲                  ▲
       │                                          │                  │
       │            ┌─────────────────────────────┴──────────────────┘
       ▼            ▼
┌─────────────────────────┐
│                         │
│        Redis            │
│ (Message Queue & Cache) │
│                         │
└─────────────────────────┘
```


# 1. Justification du modèle Map-Reduce

Nous avons choisi le modèle Map-Reduce car:

- Il permet une parallélisation efficace des traitements de données
- Il s'adapte naturellement au traitement par ville (chaque ville peut être traitée séparément)
- Il facilite l'ajout de nouvelles villes sans modification du code
- Il est hautement scalable (ajout simple de workers)
- Il sépare clairement les responsabilités entre traitement parallèle et agrégation


# 2. Découpage des Conteneurs

## 1. Orchestrator

Charge les données depuis le fichier CSV

Distribue les tâches aux workers

Gère le cycle de vie du traitement


## 2. Workers (3+ instances)

Exécutent les calculs en parallèle sur un sous-ensemble de données

Traitent chaque lot de transactions indépendamment

Stockent les résultats intermédiaires dans Redis


## 3. Aggregator

Collecte et combine tous les résultats intermédiaires

Calcule les métriques finales

Stocke les résultats dans Redis pour accès par l'API


## 4. API

Expose les résultats via des endpoints REST

Permet le filtrage des données (par ville, période, etc.)

Déclenche de nouveaux traitements à la demande


## 5. Redis

Sert de système de messagerie entre les composants

Stocke temporairement les données intermédiaires

Stocke les résultats finaux pour un accès rapide



# 3. Déployer l'architecture

```bash
autoconnect/
├── docker-compose.yml
├── data/
│   └── transactions_autoconnect.csv
├── orchestrator/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py
├── worker/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py
├── aggregator/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py
└── api/
    ├── Dockerfile
    ├── requirements.txt
    └── main.py
```

### Lancement

Pour lancer l'architecture, placez-vous dans le répertoire de projet et exécutez la commande suivante :

```bash
docker-compose build
docker-compose up -d
```

### Vérification

Pour vérifier que tout fonctionne correctement, accédez à l'API via l'URL suivante :

```bash 
# Vérifier que tous les conteneurs sont en cours d'exécution
docker-compose ps

# Consulter les log`
docker-compose logs -f
```
