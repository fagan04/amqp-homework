# Running Cluster
## Init
```bash
echo "ERLANG_COOKIE=$(openssl rand -hex 32)" > .env && chmod 600 .env
```
## Deploy cluster
```bash
docker compose up -d
```
## Cluster info
- Default username and password for all components: `admin`
- RabbitMQ cluster addresses: `localhost:5672-5674`
- Grafana dashboard: `localhost:3000`

