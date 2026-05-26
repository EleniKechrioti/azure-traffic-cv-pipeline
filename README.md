# Azure Cloud-Native Traffic Video Analytics Pipeline

A cloud-native, event-driven pipeline on **Azure** that ingests a 32-minute traffic camera video, preprocesses it into 2-minute clips, runs parallel Computer Vision inference (YOLOv8 + OpenCV), streams real-time speed violation alerts, and produces batch analytics reports — with live business metrics visualised in Prometheus + Grafana.

## Architecture

```
Raw Video → Blob Storage → Azure Function (blob trigger)
         → Container App Job (FFmpeg preprocessing → 2-min clips)
         → Event Hubs (clips-topic)
         → Container Apps CV Workers (YOLOv8 + OpenCV, horizontal scaling ×16)
              ├── Speed > 140 km/h → Event Hubs (violations-topic) → Alert Function → log
              ├── Raw JSON results → Blob Storage → Batch Report Function → report
              └── CV latency histogram → Prometheus Pushgateway → Grafana
```

![Architecture Diagram](overall_process.md)

## Components

| Component | Technology | Purpose |
|---|---|---|
| Ingestion trigger | Azure Function (blob trigger, Python) | Detects video upload, starts preprocessing job |
| Preprocessing | Container App Job (FFmpeg) | Splits 32-min video into 2-min clips |
| Messaging | Azure Event Hubs (Kafka-compatible) | Decouples preprocessing from CV workers |
| CV Workers | Container Apps (YOLOv8 + OpenCV) | Detection, tracking, speed estimation with homography |
| Real-time alerts | Azure Function (Event Hub trigger) | Logs vehicles exceeding 140 km/h |
| Batch reporting | Azure Function (Timer trigger) | Aggregates analytics across all clips |
| Metrics | Prometheus + Grafana (Container Apps) | Live business metrics during processing |

## CV Analytics Produced

1. Speed per vehicle
2. Truck percentage per lane
3. Speed limit violators (cars >90 km/h, trucks >80 km/h)
4. Real-time alerts for vehicles >140 km/h
5. Vehicle count per lane per 5-minute window
6. Top 10 fastest / slowest vehicles per lane per 5-minute window
7. Average speed per lane per 5-minute window
8. Total trucks outside the far-left lane

## Stack

- **Azure**: Function Apps, Container Apps, Event Hubs, Blob Storage
- **CV**: YOLOv8 (Ultralytics), OpenCV, perspective homography for accurate speed
- **Infra-as-Code**: Terraform (azurerm 4.x)
- **Observability**: Prometheus Pushgateway + Grafana (Container Apps)
- **Runtime**: Python 3.11, Docker

## Structure

```
functions/          # Azure Function App (blob trigger + alert + batch report)
preprocessing/      # FFmpeg preprocessing container (Dockerfile + preprocess.py)
overall_process.md  # Full architecture description with Mermaid diagram
```

## Deployment

Infrastructure is managed via Terraform. The preprocessing container is published to Docker Hub as `kanellaman/vana-preprocessor:latest`.
