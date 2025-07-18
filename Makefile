k6-test:
	k6 run -e MAX_REQUESTS=850 tests/rinha.js

docker-prune:
	docker compose down
	docker compose -f payment-processors/docker-compose.yml down
	docker system prune -fa
	docker volume prune -fa

start-payment-processors:
	docker compose -f payment-processors/docker-compose.yml down
	docker compose -f payment-processors/docker-compose.yml up -d

start-backend-build:
	docker compose down
	docker compose up --build

start-backend-build-detached:
	docker compose down
	docker compose up --build -d
	sleep 10

start-backend:
	docker compose down
	docker compose up

ci-k6-test: start-payment-processors start-backend-build-detached k6-test

build-image:
	docker build . -t distanteagle16/rinhabackend3

push-image:
	docker push distanteagle16/rinhabackend3