# Temporal Cloud metrics and history

Temporal Cloud metrics show service health; SDK metrics show worker capacity and execution behavior. Workflow histories need a separate path for retention and SQL queries. This repository provides Java, Spring Boot, and Python examples for collecting both into Elastic and Postgres, with S3 export as the recommended history archive.

## Guides

- **[ELK observability →](elk/README.md)** — connection configuration, API-key/mTLS authentication, optional Spring Boot worker, Metricbeat, and Kibana dashboards. No Prometheus server required.
- **[Audit and workflow history →](audit/README.md)** — S3 export, archive-on-close, scheduled sweeps, and tradeoffs. Uses the same Temporal connection configuration.

Start with the [example stack](elk/README.md#local-setup). These are reference implementations; review the [ingestion limits](audit/README.md#sample-limits-and-validation) before adoption. Critical business outcomes belong in the application's database for immediate querying.

[Verification checks](.github/workflows/verify.yml) · [MIT license](LICENSE).
