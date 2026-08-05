# =============================================================================
# SciPulse — Makefile
# Commandes de gestion de l'infrastructure Docker
# =============================================================================

.PHONY: help up down restart logs status validate clean

help:
	@echo "Commandes disponibles :"
	@echo "  make up        Démarrer Elasticsearch et Kibana"
	@echo "  make down      Arrêter les conteneurs"
	@echo "  make restart   Redémarrer les conteneurs"
	@echo "  make logs      Afficher les logs"
	@echo "  make status    Afficher l'état des conteneurs"
	@echo "  make validate  Vérifier le fichier Docker Compose"
	@echo "  make clean     Supprimer les conteneurs et les volumes"

up:
	docker compose up -d
	@echo ""
	@echo "Stack SciPulse démarrée"
	@echo "Elasticsearch : http://localhost:9200"
	@echo "Kibana        : http://localhost:5601"

down:
	docker compose down

restart:
	docker compose down
	docker compose up -d

logs:
	docker compose logs -f

status:
	docker compose ps

validate:
	docker compose config

clean:
	docker compose down --volumes --remove-orphans



