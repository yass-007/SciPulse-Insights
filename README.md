# SciPulse Insights

## Plateforme Big Data d'analyse de l'impact scientifique

**SciPulse Insights** est une plateforme de Data Engineering permettant de collecter, contrôler, transformer, enrichir et visualiser des données liées aux publications scientifiques.

Le projet combine trois sources principales :

- **ArXiv** pour les métadonnées des publications scientifiques ;
- **OpenAlex** pour les données de citations ;
- **Hacker News** pour intégrer un signal de visibilité communautaire.

L'architecture repose sur le modèle **Medallion Bronze / Silver / Gold** et utilise notamment **Apache Airflow, Apache Spark, MinIO, Great Expectations, Elasticsearch, Kibana et Docker**.

Une extension **Spark Structured Streaming** a également été développée afin de traiter progressivement les nouveaux fichiers Hacker News dès leur arrivée dans MinIO.

---

## Sommaire

- [Objectifs](#objectifs)
- [Architecture](#architecture)
- [Stack technique](#stack-technique)
- [Sources de données](#sources-de-données)
- [Pipeline de données](#pipeline-de-données)
- [Orchestration avec Airflow](#orchestration-avec-airflow)
- [Elasticsearch et Kibana](#elasticsearch-et-kibana)
- [Option A — Spark Structured Streaming](#option-a--spark-structured-streaming)
- [Résultats obtenus](#résultats-obtenus)
- [Structure du projet](#structure-du-projet)
- [Installation et lancement](#installation-et-lancement)
- [Accès aux services](#accès-aux-services)
- [Conclusion](#conclusion)

---

# Objectifs

SciPulse Insights a pour objectif de mettre en œuvre une chaîne Data Engineering complète allant de l'ingestion des données jusqu'à leur exploitation analytique.

Le projet permet de :

- ingérer plusieurs sources de données hétérogènes ;
- conserver les données brutes dans une couche Bronze ;
- automatiser les contrôles de qualité ;
- nettoyer, normaliser et dédupliquer les données ;
- produire des datasets Silver au format Parquet ;
- croiser les différentes sources avec Apache Spark ;
- calculer des métriques temporelles ;
- analyser le contenu textuel avec TF-IDF ;
- construire un score d'impact composite ;
- produire une couche Gold enrichie ;
- indexer les résultats dans Elasticsearch ;
- construire des dashboards dans Kibana ;
- orchestrer les traitements avec Apache Airflow ;
- expérimenter une ingestion incrémentale avec Spark Structured Streaming.

---

# Architecture

Le projet suit une architecture **Medallion**.

```text
                         SOURCES
                            │
          ┌─────────────────┼─────────────────┐
          │                 │                 │
        ArXiv            OpenAlex        Hacker News
          │                 │                 │
          └─────────────────┼─────────────────┘
                            │
                            ▼
                  ┌──────────────────┐
                  │      BRONZE      │
                  │   Données RAW    │
                  └────────┬─────────┘
                           │
                           ▼
                  Great Expectations
                   Contrôle qualité
                           │
                           ▼
                  ┌──────────────────┐
                  │      SILVER      │
                  │ Données nettoyées│
                  └────────┬─────────┘
                           │
                           ▼
                     Apache Spark
                           │
             ┌─────────────┼─────────────┐
             │             │             │
          Jointures     Métriques      TF-IDF
                        temporelles
             │             │             │
             └─────────────┼─────────────┘
                           │
                           ▼
                      Impact Score
                           │
                           ▼
                  ┌──────────────────┐
                  │       GOLD       │
                  │ Données enrichies│
                  └────────┬─────────┘
                           │
                           ▼
                     Elasticsearch
                           │
                           ▼
                        Kibana
```

**Apache Airflow** orchestre les différentes étapes du pipeline et **MinIO** fournit le stockage objet compatible S3 utilisé par les traitements.

---

# Stack technique

| Technologie | Rôle dans le projet |
|---|---|
| Python | Ingestion, nettoyage et traitements |
| Apache Airflow | Orchestration des pipelines |
| Apache Spark | Traitements distribués |
| Spark Structured Streaming | Traitement incrémental de Hacker News |
| MinIO | Stockage objet compatible S3 |
| Great Expectations | Contrôle qualité des données |
| Pandas | Transformations intermédiaires |
| Parquet | Format de stockage analytique |
| Elasticsearch | Indexation de la couche Gold |
| Kibana | Exploration et visualisation |
| Logstash | Composant de la stack de traitement |
| PostgreSQL | Base utilisée par l'infrastructure |
| Docker / Docker Compose | Conteneurisation des services |
| Git | Versionnement du projet |

---

# Sources de données

## ArXiv

ArXiv constitue la source principale du corpus scientifique.

Les données permettent notamment d'exploiter :

- les identifiants des publications ;
- les titres ;
- les résumés ;
- les auteurs ;
- les catégories ;
- les dates de publication.

Principaux composants :

```text
airflow/dags/arxiv_bronze_ingestion.py
airflow/dags/arxiv_silver_pipeline.py
src/silver/clean_arxiv.py
```

---

## OpenAlex

OpenAlex est utilisé pour enrichir les publications avec des informations relatives aux **citations scientifiques**.

Principaux composants :

```text
src/citations/ingest_openalex.py
airflow/dags/citations_silver_pipeline.py
src/silver/clean_citations.py
```

Ces données sont ensuite rapprochées des publications ArXiv lors des traitements Spark.

---

## Hacker News

Hacker News apporte un signal complémentaire permettant d'étudier la visibilité de certaines publications ou thématiques au sein d'une communauté technologique.

Principaux composants :

```text
airflow/dags/hn_bronze_ingestion.py
airflow/dags/hn_silver_pipeline.py
src/silver/clean_hn.py
```

Hacker News est également la source utilisée pour l'extension **Spark Structured Streaming**.

---

# Pipeline de données

## 1. Bronze — ingestion

La couche **Bronze** conserve les données dans un format aussi proche que possible de leur état d'origine.

Elle permet notamment :

- de préserver les données sources ;
- de séparer ingestion et transformation ;
- de conserver un historique ;
- de rejouer les traitements en aval sans nécessairement réinterroger les sources externes.

Les données sont stockées dans **MinIO**.

---

## 2. Contrôle qualité

Les données font l'objet de contrôles avec **Great Expectations** avant leur exploitation analytique.

Les suites de validation sont disponibles dans :

```text
great_expectations/expectations/
├── arxiv_bronze_suite.json
├── citations_bronze_suite.json
└── hn_bronze_suite.json
```

Les checkpoints correspondants sont définis dans :

```text
great_expectations/checkpoints/
├── arxiv_bronze_checkpoint.yml
├── citations_bronze_checkpoint.yml
└── hn_bronze_checkpoint.yml
```

Les scripts de profiling et de validation sont regroupés dans :

```text
src/quality/
```

Cette étape permet de détecter les anomalies avant leur propagation vers les couches suivantes.

---

## 3. Silver — nettoyage

La couche **Silver** contient les datasets nettoyés et normalisés.

Les traitements appliquent notamment :

- la sélection des champs utiles ;
- la normalisation des types ;
- la gestion des valeurs manquantes ;
- la normalisation des dates ;
- la déduplication ;
- la préparation des données pour Spark.

Les scripts principaux sont :

```text
src/silver/
├── clean_arxiv.py
├── clean_citations.py
└── clean_hn.py
```

Les datasets Silver sont ensuite stockés au format **Parquet**.

---

## 4. Traitements Spark

Les datasets Silver sont ensuite exploités avec **Apache Spark**.

Les traitements sont regroupés dans :

```text
src/spark/
├── load_silver_sources.py
├── join_sources.py
├── compare_join_strategies.py
├── temporal_metrics.py
├── tfidf_pipeline.py
├── impact_score.py
└── hn_streaming.py
```

### Jointures multi-sources

`join_sources.py` rapproche les données ArXiv, OpenAlex et Hacker News afin de construire un dataset enrichi.

```text
ArXiv
   │
   ├──────── Citations
   │
   └──────── Hacker News
   │
   ▼
Dataset scientifique enrichi
```

`compare_join_strategies.py` permet également d'étudier différentes stratégies de jointure Spark et leurs implications en matière de traitement distribué.

### Métriques temporelles

`temporal_metrics.py` exploite la dimension temporelle des publications afin de construire des indicateurs complémentaires.

### TF-IDF

`tfidf_pipeline.py` utilise **TF-IDF (Term Frequency — Inverse Document Frequency)** afin de produire une représentation du contenu textuel des publications.

Le principe est :

```text
Texte
  ↓
Tokenisation
  ↓
Term Frequency
  ↓
IDF
  ↓
Vecteur TF-IDF
```

Cette représentation permet d'identifier les termes caractéristiques des documents du corpus.

---

## 5. Gold — score d'impact

La couche **Gold** contient le dataset final enrichi.

Le calcul principal est réalisé dans :

```text
src/spark/impact_score.py
```

Le score d'impact combine plusieurs dimensions normalisées :

```text
Signal académique
citation_signal_norm
        │
        ├──────────┐
        │          │
Signal HN          │
hn_signal_norm ────┼──► Impact Score
        │          │
Signal de récence  │
recency_signal_norm
```

Le dataset Gold contient notamment :

```text
arxiv_id
title
published_date
cited_by_count
hn_score_total
hn_descendants_total
citation_signal_norm
hn_signal_norm
recency_signal_norm
impact_score
```

L'objectif est de proposer une mesure multidimensionnelle de l'impact, plutôt que de se limiter au nombre brut de citations.

---

# Orchestration avec Airflow

Apache Airflow orchestre les différentes étapes de SciPulse.

Les principaux DAGs sont :

```text
airflow/dags/
├── arxiv_bronze_ingestion.py
├── arxiv_silver_pipeline.py
├── citations_silver_pipeline.py
├── hn_bronze_ingestion.py
├── hn_silver_pipeline.py
└── scipulse_end_to_end_pipeline.py
```

Le DAG principal est :

```text
scipulse_end_to_end_pipeline
```

Il permet de coordonner les différentes étapes nécessaires à la production des données finales.

Airflow permet également de :

- déclencher les pipelines ;
- visualiser les dépendances entre les tâches ;
- suivre les succès et les échecs ;
- consulter les logs ;
- rejouer certaines étapes.

---

# Elasticsearch et Kibana

## Elasticsearch

Les données Gold sont indexées dans Elasticsearch via les scripts :

```text
src/gold/
├── index_arxiv_elasticsearch.py
└── write_elasticsearch.py
```

L'index final utilisé est :

```text
arxiv-papers-enriched
```

Les mappings Elasticsearch sont versionnés dans :

```text
elasticsearch/mappings/
```

Elasticsearch permet ensuite d'effectuer des recherches et agrégations rapides sur le dataset Gold.

---

## Kibana

Kibana constitue la couche de visualisation du projet.

Les dashboards permettent notamment d'explorer :

Les objets Kibana sont exportés dans :

```text
Kibana/exports/scipulse-dashboards.ndjson
```

Ils peuvent être réimportés depuis :

```text
Kibana
  ↓
Stack Management
  ↓
Saved Objects
  ↓
Import
```

---

# Option A — Spark Structured Streaming

Une extension **Spark Structured Streaming** a été développée pour le pipeline Hacker News.

Le script correspondant est :

```text
src/spark/hn_streaming.py
```

## Objectif

Le pipeline Airflow initial fonctionne en batch.

L'extension permet à Spark de rester actif et de surveiller directement :

```text
s3a://hn-raw/
```

Lorsqu'un nouveau fichier JSON/JSONL arrive dans MinIO, il est automatiquement pris en compte lors d'un nouveau micro-batch, sans redémarrage du job Spark.

```text
hn-raw/
   │
   ▼
Spark readStream
   │
   ▼
Nettoyage Silver
   │
   ▼
Déduplication
   │
   ▼
Spark writeStream
   │
   ▼
hn-clean/streaming/
```

La sortie est écrite en **Parquet** avec :

```text
writeStream
mode = append
```

Le pipeline utilise :

```text
Source      : s3a://hn-raw/
Destination : s3a://hn-clean/streaming/
Checkpoint  : s3a://hn-clean/_checkpoints/hn_streaming_v2/
```

La configuration utilise :

```text
maxFilesPerTrigger = 10
trigger = 30 secondes
```

Le checkpoint permet de conserver l'état et la progression du stream.

Aucun service Kafka supplémentaire n'est nécessaire : MinIO est directement utilisé comme source de fichiers S3.

---

## Batch vs Streaming

| Critère | Airflow Batch | Spark Structured Streaming |
|---|---|---|
| Déclenchement | DAG Airflow | Job Spark actif |
| Lecture | Batch | `readStream` |
| Nouveaux fichiers | Prochaine exécution | Détection automatique |
| Latence | Dépend du planning Airflow | Micro-batch toutes les 30 s |
| Sortie | Parquet | Parquet |
| Stockage | MinIO | MinIO |
| État | Airflow | Checkpoint Spark |
| Kafka | Non | Non |

Le batch est adapté aux traitements planifiés et reproductibles.

Le Structured Streaming permet de réduire le délai entre l'arrivée d'un nouveau fichier et son traitement.

---

## Validation du streaming

Le fonctionnement de l'extension a été validé en maintenant le job Spark actif puis en ajoutant un nouveau fichier dans `hn-raw/`.

Fichier de test :

```text
hn_streaming_test_v2.jsonl
```

Événement injecté :

```text
id    = 888888888
by    = scipulse_test_v2
title = SCIPULSE_STREAMING_LIVE_TEST
```

Le fichier a été détecté et traité automatiquement.

La vérification du dataset produit a retourné :

```text
SCIPULSE STREAMING LIVE TEST

id    : 888888888
by    : scipulse_test_v2
title : SCIPULSE_STREAMING_LIVE_TEST

MATCHING ROWS: 1
```

Ce test confirme que le nouveau fichier a été traité **sans redémarrage du job Spark**.

---

# Résultats obtenus

Lors de l'exécution du pipeline Gold :

| Dataset / étape | Volume |
|---|---:|
| ArXiv | 3 127 797 lignes |
| Publications avec citations | 44 022 |
| Publications associées à Hacker News | 91 |
| Dataset Gold final | **44 023 lignes** |

Les **44 023 documents Gold** ont ensuite été écrits dans :

```text
arxiv-papers-enriched
```

Exemple de résultats obtenus :

| Publication | Impact Score |
|---|---:|
| A Watermark for Large Language Models | 54.07 |
| Natural Language Processing (almost) from Scratch | 53.64 |
| Very Deep Convolutional Networks for Large-Scale Image Recognition | 51.72 |
| Scikit-learn: Machine Learning in Python | 50.64 |

Ces résultats illustrent la combinaison des signaux académiques, communautaires et temporels dans le calcul du score.

---

# Structure du projet

```text
SciPulse-Insights/
│
├── airflow/
│   └── dags/
│       ├── arxiv_bronze_ingestion.py
│       ├── arxiv_silver_pipeline.py
│       ├── citations_silver_pipeline.py
│       ├── hn_bronze_ingestion.py
│       ├── hn_silver_pipeline.py
│       └── scipulse_end_to_end_pipeline.py
│
├── docker/
│   ├── logstash/
│   └── postgres/
│
├── elasticsearch/
│   └── mappings/
│
├── great_expectations/
│   ├── checkpoints/
│   └── expectations/
│
├── Kibana/
│   └── exports/
│       └── scipulse-dashboards.ndjson
│
├── src/
│   ├── citations/
│   ├── gold/
│   ├── quality/
│   ├── silver/
│   └── spark/
│       ├── compare_join_strategies.py
│       ├── hn_streaming.py
│       ├── impact_score.py
│       ├── join_sources.py
│       ├── load_silver_sources.py
│       ├── temporal_metrics.py
│       └── tfidf_pipeline.py
│
├── docker-compose.yml
├── Makefile
├── requirements.txt
├── .gitignore
└── README.md
```

Les logs, caches Python et autres fichiers générés ne sont pas représentés dans cette arborescence.

---

# Installation et lancement

## Prérequis

Les outils suivants sont nécessaires :

- Git ;
- Docker ;
- Docker Compose ;
- Python 3 ;
- pip.

---

## 1. Cloner le projet

```bash
git clone <URL_DU_REPOSITORY>
cd SciPulse-Insights
```

---

## 2. Installer les dépendances Python

Il est recommandé d'utiliser un environnement virtuel :

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## 3. Démarrer l'infrastructure

```bash
docker compose up -d
```

Vérifier l'état des conteneurs :

```bash
docker compose ps
```

En cas de problème :

```bash
docker compose logs -f
```

---

## 4. Lancer les pipelines

Une fois les services démarrés, ouvrir l'interface Airflow puis utiliser les DAGs correspondant aux différentes étapes du projet.

Le pipeline principal est :

```text
scipulse_end_to_end_pipeline
```

Le flux général est :

```text
Ingestion
   ↓
Bronze
   ↓
Quality
   ↓
Silver
   ↓
Spark
   ↓
Gold
   ↓
Elasticsearch
   ↓
Kibana
```

---

# Accès aux services

Une fois Docker démarré, les principaux services sont accessibles localement.

| Service | Adresse locale | Rôle |
|---|---|---|
| Apache Airflow | `http://localhost:8080` | Orchestration |
| MinIO Console | `http://localhost:9001` | Exploration du stockage |
| MinIO API | `http://localhost:9000` | Endpoint S3 |
| Elasticsearch | `http://localhost:9200` | Index Gold |
| Kibana | `http://localhost:5601` | Dashboards |

> Les ports correspondent à la configuration locale du projet et peuvent être vérifiés dans `docker-compose.yml`.

---

## Vérifications rapides

### Docker

```bash
docker compose ps
```

### Elasticsearch

```bash
curl http://localhost:9200
```

### Nombre de documents Gold

```bash
curl "http://localhost:9200/arxiv-papers-enriched/_count?pretty"
```

### Fichiers produits par le streaming

```bash
docker exec scipulse-minio \
  mc ls --recursive local/hn-clean/streaming/ \
  | grep '\.parquet$'
```

---

# Conclusion

SciPulse Insights met en œuvre une chaîne Data Engineering complète :

```text
COLLECTER
    ↓
STOCKER
    ↓
VALIDER
    ↓
NETTOYER
    ↓
CROISER
    ↓
ENRICHIR
    ↓
INDEXER
    ↓
VISUALISER
```

L'architecture **Bronze / Silver / Gold** permet de séparer clairement les différentes étapes du cycle de vie des données.

**Apache Airflow** assure l'orchestration, **MinIO** le stockage objet, **Great Expectations** le contrôle qualité et **Apache Spark** les traitements distribués.

Les données enrichies sont ensuite indexées dans **Elasticsearch** et explorées dans **Kibana**.

Enfin, l'extension **Spark Structured Streaming** complète l'architecture batch en permettant de détecter et traiter de nouveaux fichiers Hacker News pendant l'exécution du job, sans nécessiter son redémarrage.

