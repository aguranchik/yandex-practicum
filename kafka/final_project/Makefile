.PHONY: certs config test up analytics demo down

certs:
	bash scripts/generate-certificates.sh

config:
	docker compose config --quiet

test:
	env GOCACHE=/private/tmp/gocache-final-project go test ./...

up:
	docker compose up -d --build

analytics:
	bash scripts/run-analytics.sh

demo:
	bash scripts/test-e2e.sh

down:
	docker compose --profile analytics --profile tools down
