# Presentation evidence

Captured from the local AWS-compatible stack running through LocalStack. This is not evidence of an AWS deployment.

- `grafana-load-dashboard.png`: progressive benchmark dashboard captured before the failure campaign
- `grafana-failure-dashboard.png`: dashboard after transient, poison, silent-drop, recovery, and backlog scenarios
- `locust-100-users.png`: peak-throughput Locust report
- `locust-250-users.png`: saturation/tail-latency Locust report
- `prometheus-alerts.png` and `alertmanager-alerts.png`: active local alert evidence
- `localstack-evidence.png`: queue, DLQ depth, and DLQ message evidence
- `reliability-evidence.png`: worker recovery and reconciliation log evidence
- JSON and log files: raw machine-readable/readable sources for the screenshots
