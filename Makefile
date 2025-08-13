k6-test-html:
	K6_WEB_DASHBOARD=true \
	K6_WEB_DASHBOARD_PORT=5665 \
	K6_WEB_DASHBOARD_PERIOD=1s \
	K6_WEB_DASHBOARD_OPEN=true \
	K6_WEB_DASHBOARD_EXPORT=report.html \
	k6 run -e MAX_REQUESTS=550 tests/rinha.js

k6-test:
	k6 run -e MAX_REQUESTS=550 tests/rinha.js

docker-prune:
	docker compose down
	docker compose -f payment-processors/docker-compose.yml down
	docker system prune -fa
	docker volume prune -fa

start-payment-processors:
	docker compose -f payment-processors/docker-compose.yml down
	docker compose -f payment-processors/docker-compose.yml up -d

start-backend:
	docker compose down
	docker compose up

start-backend-build:
	docker compose down
	docker compose up --build

start-backend-build-detached:
	docker compose down
	docker compose up --build -d
	sleep 15

start-backend:
	docker compose down
	docker compose up

setup-docker:
	sudo systemctl start docker
	sudo chmod 666 /var/run/docker.sock

setup-local: docker-prune start-payment-processors start-backend-build

ci-k6-test: start-payment-processors start-backend-build-detached
	k6 run -e MAX_REQUESTS=550 tests/rinha.js

build-image:
	docker build . -t distanteagle16/rinhabackend3-api:1.3.1 -f ./docker/Dockerfile.api.x86_64-unknown-linux-musl
	docker build . -t distanteagle16/rinhabackend3-worker:1.3.1 -f ./docker/Dockerfile.worker.x86_64-unknown-linux-musl

push-image:
	docker push distanteagle16/rinhabackend3-api:1.3.1
	docker push distanteagle16/rinhabackend3-worker:1.3.1