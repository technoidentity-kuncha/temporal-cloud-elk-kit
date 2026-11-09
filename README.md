# Temporal Cloud metrics and history

Reference implementations and setup guidance for senior engineers working with Temporal Cloud.

## What this repository covers

- **Observability:** Cloud and SDK metrics collected by Metricbeat into Elastic, with Kibana dashboards. No Prometheus server required.
- **Worker examples:** Java and Python, with an optional Spring Boot worker.
- **Connection configuration:** shared Temporal profiles, API keys, and mTLS authentication.
- **History retention and querying:** recommended S3 export, with optional Postgres ingestion for SQL access.
- **Application-owned alternatives:** archive-on-close interceptors and Temporal-scheduled sweeps, with components, tradeoffs, and code references.

## Reading guide

- **[ELK observability →](elk/README.md)** — configuration, workers, metrics, and dashboards.
- **[Audit and workflow history →](audit/README.md)** — retention approaches, ingestion, and implementation limits.

Run the [example stack](elk/README.md#local-setup). These are reference implementations; review the [ingestion limits](audit/README.md#sample-limits-and-validation) before adoption. Critical business outcomes belong in the application's database for immediate querying.

[CI checks](.github/workflows/verify.yml) · [MIT license](LICENSE).
