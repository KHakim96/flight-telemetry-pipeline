#!/usr/bin/env bash
mkdir -p terraform airflow/dags airflow/logs airflow/plugins producer consumer dbt && touch .gitignore .env.example docker-compose.yml deploy.sh terraform/main.tf terraform/variables.tf terraform/outputs.tf producer/Dockerfile producer/main.py consumer/Dockerfile consumer/main.py
