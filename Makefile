.PHONY: build up down chaos bench charts clean
build:
	mkdir -p bin
	go build -o bin/producer ./producer
	go build -o bin/consumer ./consumer
	go build -o bin/clustermon ./cmd/clustermon
up:
	cd rabbitmq-cluster && test -f .env || (echo "ERLANG_COOKIE=$$(openssl rand -hex 32)" > .env && chmod 600 .env)
	cd rabbitmq-cluster && docker compose up -d
down:
	cd rabbitmq-cluster && docker compose down
chaos: build
	scripts/chaos.sh all
bench: build
	scripts/bench.sh all
charts:
	python3 scripts/plot_results.py
clean:
	rm -rf bin
