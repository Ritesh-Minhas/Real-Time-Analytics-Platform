# Real-Time Data Analytics Platform

This project implements a real-time data analytics platform capable of streaming data ingestion, anomaly detection, and real-time visualization.

## Technologies
Python
Kafka
PostgreSQL
Streamlit
Scikit-learn

## Architecture
Producers → Kafka → Bronze → Silver → Gold → Detection → Dashboard

## Features
• Real-time streaming pipeline
• Data quality framework
• Rule-based anomaly detection
• ML anomaly detection (Isolation Forest)
• Evaluation metrics (Precision/Recall/F1)
• Batch vs Stream reconciliation
• Root cause tagging
• Interactive dashboard

## Run Instructions

Start infrastructure
docker compose up

Start processors
python -m services.processing.silver_processor
python -m services.processing.gold_aggregator

Start dashboard
streamlit run dashboard/app.py