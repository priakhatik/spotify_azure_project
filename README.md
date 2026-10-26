# Spotify Azure Data Engineering Project 🎧

An end-to-end data engineering pipeline that ingests, transforms, and models Spotify-style streaming data using **Azure Data Factory**, **Azure Data Lake Storage Gen2**, and **Azure Databricks** (with **Lakeflow / Delta Live Tables**). The project follows a medallion architecture (Bronze → Silver → Gold) and is fully orchestrated with parameterized, metadata-driven pipelines.

---

## 🏗️ Architecture

```
Source Data (SQL DB / Files)
        │
        ▼
 Azure Data Factory  ──►  ADLS Gen2 (Bronze)
        │                        │
        │                        ▼
        │              Databricks Autoloader
        │                        │
        │                        ▼
        │                ADLS Gen2 (Silver)
        │                        │
        │                        ▼
        │          Lakeflow / Delta Live Tables (CDC + SCD Type 2)
        │                        │
        │                        ▼
        └────────────────►  Unity Catalog (Gold)
                                  │
                                  ▼
                          Power BI / Analytics
```

## 🔧 Tech Stack

| Layer | Tools |
|---|---|
| Orchestration | Azure Data Factory (pipelines, linked services, datasets) |
| Storage | Azure Data Lake Storage Gen2 (bronze / silver containers) |
| Compute & Transformation | Azure Databricks, PySpark, Autoloader |
| Streaming / CDC | Delta Live Tables (Lakeflow Pipelines), `create_auto_cdc_flow` |
| Governance | Unity Catalog (catalogs, schemas, storage credentials, external locations) |
| Templating | Jinja2 (dynamic SQL generation) |
| Source DB | Azure SQL Database |

## 📁 Repository Structure

```
spotify_azure_project/
├── dataset/              # ADF dataset definitions
├── factory/              # ADF factory-level config
├── linkedService/        # ADF linked services (ADLS, SQL DB)
├── pipeline/             # ADF pipelines (incremental load, loop, etc.)
├── spotify_dab/          # Databricks Asset Bundle
│   ├── src/
│   │   └── med_architecture/
│   │       ├── silver/          # Silver-layer transformations
│   │       └── gold/
│   │           └── dlt/
│   │               ├── transformations/   # DLT pipeline source (DimUser, DimArt, DimTrack, FactStream etc.)
│   │               ├── explorations/       # Ad-hoc exploration notebooks
│   │               └── utilities/          # Shared pipeline utilities
│   ├── utils/            # Reusable Python transformation helpers
│   ├── jinja/             # Jinja-based dynamic SQL notebook
│   ├── databricks.yml     # Bundle configuration (dev/prod targets)
│   └── pyproject.toml
└── README.md
```

## ⚙️ Pipeline Overview

1. **Ingestion (ADF):** Parameterized pipelines pull source data (e.g. `DimUser`, `DimTrack`, `DimArtist`, `FactStream`) into the **Bronze** layer of ADLS Gen2, supporting both full and incremental loads via a loop-based control pipeline.
2. **Bronze → Silver (Databricks Autoloader):** PySpark Structured Streaming with `cloudFiles` incrementally ingests raw Parquet files, applies schema evolution, and lands cleaned data in the **Silver** layer as Delta tables.
3. **Silver → Gold (Lakeflow Pipelines):** Declarative pipeline code imports `pyspark.pipelines as dp`, uses `dp.create_streaming_table`, and applies `dp.create_auto_cdc_flow` for **CDC (Change Data Capture)** and **SCD Type 2** history tracking. `DimUser` sequences changes by the same `updated_at` watermark used by the ADF incremental-ingestion pipeline.
4. **Dynamic SQL Generation:** A Jinja2-templated notebook builds parameterized `SELECT`/`JOIN` queries across dimension and fact tables for flexible ad-hoc analysis.

## 🔐 Unity Catalog Setup

This project uses **Unity Catalog** with explicit, catalog-managed storage rather than relying on default metastore root storage:

- An **Azure Databricks Access Connector** is granted `Storage Blob Data Contributor` on the ADLS Gen2 account.
- A **Storage Credential** references that connector via Azure Managed Identity.
- An **External Location** and catalog/schema-level **Managed Location** point Unity Catalog explicitly at the `gold` container.

## 🚀 Getting Started

```bash
git clone https://github.com/priakhatik/spotify_azure_project.git
cd spotify_azure_project/spotify_dab
databricks bundle validate
databricks bundle deploy --target dev
```

## 🔒 Alert webhook configuration

The ADF `spotify_incremental_loop` pipeline no longer stores a signed Logic App webhook URL in source control. Supply the `alert_webhook_url` **SecureString** parameter at runtime through a trusted deployment/orchestration process; do not put the signed URL in parameter defaults, committed JSON, or CI logs. The Alerts Web activity has secure input and output logging enabled.

**Required maintainer action:** The previous signed webhook URL was committed to Git history. Removing it from the current file does **not** revoke the old signature or erase it from historical commits. Rotate/revoke that Logic App callback URL in Azure, update the protected runtime value, and check for unauthorized invocations. The pipeline's alerting will not work until a valid replacement is provided.

Run `python -m unittest discover -s tests -v` to check the JSON configuration and catch accidental reintroduction of inline signed URLs. This is a static configuration test, not an Azure integration test.

## 📌 Notes

- Built as a hands-on portfolio project to demonstrate end-to-end Azure data engineering: orchestration, incremental ingestion, streaming transformations, CDC/SCD modeling, and Unity Catalog governance.
- Designed with a deliberately modular structure (silver/gold separation, reusable utils, DAB-based deployment) to reflect production-style project organization rather than a single monolithic notebook.

---

**Author:** [Priya Khatik](https://github.com/priakhatik)
