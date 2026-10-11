.PHONY: up down migrate run test test-crash test-idem test-outbox bench psql build

up:
	docker compose up -d --build

down:
	docker compose down -v

build:
	./mvnw -q -B package -DskipTests

migrate:
	./mvnw -q -B flyway:migrate \
		-Dflyway.url=jdbc:postgresql://localhost:5432/ledger \
		-Dflyway.user=ledger \
		-Dflyway.password=ledger \
		-Dflyway.locations=filesystem:src/main/resources/db/migration

run:
	DB_HOST=localhost DB_PORT=5432 DB_NAME=ledger DB_USER=ledger DB_PASSWORD=ledger ./mvnw -q spring-boot:run

test:
	./mvnw -B verify

test-crash:
	./harness/t1_crash.sh

test-idem:
	./harness/t2_sequential_dupe.sh
	./harness/t3_concurrent_dupe.sh
	./harness/t4_key_conflict.sh

test-outbox:
	./harness/t5_poison_event.sh
	./harness/t6_concurrent_drain.sh

bench:
	./bench/run.sh

psql:
	docker compose exec postgres psql -U ledger -d ledger
