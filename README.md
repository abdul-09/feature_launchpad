# 🚀 Feature Launchpad

> **End-to-end data engineering platform for measuring product feature impact**

[![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)](https://python.org)
[![Kafka](https://img.shields.io/badge/Kafka-3.6-orange?logo=apachekafka)](https://kafka.apache.org)
[![Spark](https://img.shields.io/badge/Spark-3.5-yellow?logo=apachespark)](https://spark.apache.org)
[![dbt](https://img.shields.io/badge/dbt-1.7-orange?logo=dbt)](https://getdbt.com)
[![DuckDB](https://img.shields.io/badge/DuckDB-0.10-yellow)](https://duckdb.org)

---

## 📋 Problem Statement

Product teams need to **quantify feature impact** beyond vanity metrics. When launching a new feature:

- How do we know if users are *actually* engaging with it?
- What's the **business impact** on retention and conversion?
- Where are users dropping off in the experience?
- How do we measure success in near real-time?

Feature Launchpad solves this by providing a complete data infrastructure for **event-driven product analytics**.

---

## 💡 Solution Overview

Feature Launchpad is a **production-grade data pipeline** that demonstrates how to build a complete analytics system from scratch. It features:

1. **An Interactive Product** - A Product Recommendation Configurator that captures rich user interactions
2. **Event Instrumentation** - A type-safe tracking SDK with batching and offline support
3. **Streaming Pipeline** - Real-time event processing with exactly-once semantics
4. **Analytics Layer** - dbt transformations computing business metrics
5. **Impact Dashboard** - Visualizations proving feature value with retention lift analysis

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              FEATURE LAUNCHPAD                                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌────────────┐ │
│  │   Frontend   │────▶│  Backend API │────▶│    Kafka     │────▶│   Spark    │ │
│  │  Next.js +   │     │   FastAPI    │     │   Streaming  │     │  Streaming │ │
│  │  React + TS  │     │  + Pydantic  │     │    Queue     │     │  Processor │ │
│  └──────────────┘     └──────────────┘     └──────────────┘     └─────┬──────┘ │
│         │                                                              │        │
│         │  Event Tracking SDK                                          ▼        │
│         │  • Auto session mgmt                               ┌──────────────┐   │
│         │  • Batching & retry                                │   Parquet    │   │
│         │  • Type safety                                     │  Data Lake   │   │
│         │                                                    └──────┬───────┘   │
│         │                                                           │           │
│         │                                                           ▼           │
│         │                                                    ┌──────────────┐   │
│         │                                                    │  dbt Core    │   │
│         │                                                    │  Transform   │   │
│         │                                                    └──────┬───────┘   │
│         │                                                           │           │
│         ▼                                                           ▼           │
│  ┌──────────────┐                                           ┌──────────────┐   │
│  │  Streamlit   │◀──────────────────────────────────────────│   DuckDB     │   │
│  │  Dashboard   │              Query Analytics              │  Warehouse   │   │
│  └──────────────┘                                           └──────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

**[See full architecture diagram →](docs/ARCHITECTURE.md)**

---

## 📊 Key Features & Metrics

### Tracked Event Types

| Event | Description | Engagement Signal |
|-------|-------------|-------------------|
| `quiz_started` | User begins configurator | Interest |
| `slider_adjusted` | User adjusts preference slider | Active engagement |
| `option_selected` | User selects an option | Decision making |
| `quiz_completed` | User finishes all steps | Completion |
| `result_viewed` | User views recommendation | Value received |
| `result_shared` | User shares their result | Viral potential |

### Computed Metrics

| Metric | What It Measures | Business Value |
|--------|------------------|----------------|
| **Adoption Rate** | % of DAU using feature | Feature discovery |
| **Completion Rate** | Start → finish conversion | UX effectiveness |
| **Engagement Depth** | Events per session | User investment |
| **Session Duration** | Time spent interacting | Content quality |
| **Share Rate** | % who share results | Viral coefficient |
| **Retention Lift** | Return rate delta | **Business impact** |

---


## 🛠️ Tech Stack & Justification

| Component | Technology | Why This Choice |
|-----------|------------|-----------------|
| **Frontend** | Next.js + React + TypeScript | Type safety, SSR, modern DX |
| **Styling** | Tailwind CSS | Rapid iteration, consistent design |
| **API** | FastAPI + Pydantic | Async, auto-docs, schema validation |
| **Queue** | Apache Kafka | Durability, exactly-once, scalability |
| **Processing** | Spark Structured Streaming | Stateful processing, watermarking |
| **Storage** | Parquet | Columnar, compressed, schema evolution |
| **Warehouse** | DuckDB | Blazing fast OLAP, zero config |
| **Transform** | dbt Core | Version control, testing, documentation |
| **Dashboard** | Streamlit | Rapid prototyping, Python native |
| **Monitoring** | Prometheus + Grafana | Industry standard observability |

---

## 🚀 Getting Started

### Prerequisites

- Docker & Docker Compose
- Python 3.11+ (for local development)
- Node.js 18+ (for frontend development)

### Quick Start

```bash
# Clone the repository
git clone https://github.com/abdul-09/feature-launchpad.git
cd feature-launchpad

# Start all services
docker-compose up -d

# Wait for services to initialize (about 30 seconds)
sleep 30

# Generate sample event data
docker-compose exec backend python -m pipeline.scripts.simulate_events \
    --users 500 --days 14 --kafka

# Run dbt transformations
docker-compose exec dbt dbt run

# View the dashboard
open http://localhost:8501
```

### Service URLs

| Service | URL | Description |
|---------|-----|-------------|
| Dashboard | http://localhost:8501 | Analytics dashboard |
| Frontend | http://localhost:3000 | Product configurator |
| API Docs | http://localhost:8000/docs | OpenAPI documentation |
| Kafka UI | http://localhost:8080 | Topic monitoring |
| Grafana | http://localhost:3001 | Operations dashboard |

---

## 📁 Project Structure

```
feature-launchpad/
│
├── frontend/                    # Interactive web application
│   ├── src/
│   │   ├── components/          # React components
│   │   │   └── ProductConfigurator.tsx
│   │   └── lib/
│   │       └── tracking.ts      # Event tracking SDK
│   ├── package.json
│   └── Dockerfile
│
├── backend/                     # Event ingestion service
│   ├── app/
│   │   ├── api/events.py        # REST endpoints
│   │   ├── schemas/events.py    # Pydantic models
│   │   ├── services/kafka_producer.py
│   │   └── main.py
│   ├── requirements.txt
│   └── Dockerfile
│
├── pipeline/                    # Data engineering core
│   ├── streaming/
│   │   └── event_processor.py   # Spark streaming job
│   ├── dbt/
│   │   ├── models/
│   │   │   ├── staging/
│   │   │   │   └── stg_events.sql
│   │   │   └── marts/
│   │   │       ├── core/
│   │   │       │   ├── fact_events.sql
│   │   │       │   └── dim_users.sql
│   │   │       └── engagement/
│   │   │           ├── engagement_metrics.sql
│   │   │           └── funnel_analysis.sql
│   │   ├── dbt_project.yml
│   │   └── profiles.yml
│   └── scripts/
│       └── simulate_events.py   # Data generator
│
├── dashboard/                   # Analytics UI
│   ├── app.py                   # Streamlit application
│   └── Dockerfile
│
├── docs/
│   └── ARCHITECTURE.md          # System design docs
│
├── docker-compose.yml           # Full stack orchestration
└── README.md                    # You are here
```

---

## 📈 Results & Simulation

In simulated load testing with realistic user behavior patterns:

| Metric | Result |
|--------|--------|
| **Events Processed** | 100,000+ |
| **End-to-end Latency** | < 2 seconds |
| **Completion Rate** | 62% (simulated) |
| **Retention Lift** | +50-60% for completers |
| **Events per Session** | 8-12 average |

The pipeline demonstrates clear ability to:
- ✅ Capture granular user interactions
- ✅ Process events in near real-time
- ✅ Compute meaningful engagement metrics
- ✅ Quantify feature impact on retention

---

## 🔮 Future Enhancements

| Enhancement | Description | Status |
|-------------|-------------|--------|
| A/B Testing Framework | Compare feature variants | 🔜 Planned |
| Real-time User Segmentation | Dynamic cohorts | 🔜 Planned |
| Predictive Engagement Scoring | ML-based churn prediction | 🔜 Planned |
| Reverse ETL | Push insights to CRM | 💡 Idea |
| Mobile SDK | iOS/Android tracking | 💡 Idea |

---

## 🧪 Testing

```bash
# Run backend tests
cd backend && pytest

# Run dbt tests
docker-compose exec dbt dbt test

# Lint Python code
ruff check .

# Type check frontend
cd frontend && npm run typecheck
```

---

## 📖 Learn More

- [Architecture Deep Dive](docs/ARCHITECTURE.md)
- [dbt Documentation](pipeline/dbt/README.md)
- [API Reference](http://localhost:8000/docs)

---

## 👤 Author

Built by **[Abdulaziz Hussein]** — Software Engineer, passionate about building systems that turn raw data into actionable insights.

- 🔗 [LinkedIn](https://linkedin.com/in/abdulaziz-mohamed-hussein)
- 🐙 [GitHub](https://github.com/abdul-09)
- 📧 your.email@example.com

---

## 📄 License

MIT License - feel free to use this as a foundation for your own projects.

---

<p align="center">
  <strong>Feature Launchpad</strong> — Because great products deserve great analytics.
</p>
