COMPOSE := docker compose -p ledgermesh -f deploy/docker-compose.yml
MVN := mvn -B

.PHONY: help build test lint format up down logs ps chaos chaos-tight chaos-baseline demo clean

help:
	@echo "build   compile and package all modules (no tests)"
	@echo "test    unit and integration tests (Testcontainers)"
	@echo "lint    formatting check (spotless)"
	@echo "format  apply formatting"
	@echo "up      build images and start the stack"
	@echo "down    stop the stack and drop volumes"
	@echo "chaos   run the chaos test against the stack (make demo is an alias)"
	@echo "chaos-tight  the same run with twice the kills and a two second restart"
	@echo "chaos-baseline  the same load with no kills, the reference for the latency numbers"
	@echo "        CHAOS_RECORD=1 keeps a run's summary under chaos/evidence/, CHAOS_SEED repeats a schedule"

build:
	$(MVN) -DskipTests package

test:
	$(MVN) verify

lint:
	$(MVN) spotless:check

format:
	$(MVN) spotless:apply

up:
	$(COMPOSE) up -d --build --wait

down:
	$(COMPOSE) down -v --remove-orphans

logs:
	$(COMPOSE) logs -f --tail=200

ps:
	$(COMPOSE) ps

chaos:
	bash chaos/run.sh

chaos-tight:
	CHAOS_PROFILE=tight bash chaos/run.sh

chaos-baseline:
	CHAOS_KILLS=0 bash chaos/run.sh

demo: chaos

clean: down
	$(MVN) clean
