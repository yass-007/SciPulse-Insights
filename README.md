# SciPulse & SciPulse Insights

Plateforme de veille scientifique à grande échelle basée sur une architecture
DataOps, l'Elastic Stack et Apache Spark.

## Module en cours

Module 1 — SciPulse : pipeline DataOps & Elastic Stack.

## Infrastructure

L'infrastructure est mise en place progressivement avec Docker Compose.

Services actuellement disponibles :

- Elasticsearch
- Kibana

Services prévus dans les prochaines étapes :

- PostgreSQL
- MinIO
- Apache Airflow
- Logstash
- Apache Spark

## Démarrage

Créer le fichier local de configuration :

```bash
cp .env.example .env
```
Démarrer les services :

```bash
make up
```

Arrêter les services :

```bash
make down
```

## Interfaces:

Elasticsearch : http://localhost:9200
Kibana : http://localhost:5601

## Branches Git

main : version stable ;
dev : branche de développement.

