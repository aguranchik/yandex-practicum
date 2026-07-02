.PHONY: test build preflight create-cloud configure-cloud deploy e2e start-nifi stop-paid destroy

test:
	go test ./...

build:
	./scripts/build-linux.sh

preflight:
	./scripts/preflight.sh

create-cloud:
	CONFIRM_PAID_RESOURCES=YES ./scripts/create-cloud.sh

configure-cloud:
	./scripts/configure-cloud.sh

deploy:
	./scripts/deploy-to-vm.sh

e2e:
	./scripts/run-e2e.sh

start-nifi:
	./scripts/start-nifi.sh

stop-paid:
	./scripts/stop-paid-resources.sh

destroy:
	CONFIRM_DESTROY=DELETE ./scripts/destroy-cloud.sh
