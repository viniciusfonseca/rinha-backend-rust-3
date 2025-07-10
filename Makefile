k6-test:
	k6 run tests/rinha.js

start-payment-processors:
	docker compose -f payment-processors/docker-compose.yml down
	docker compose -f payment-processors/docker-compose.yml up -d

start-backend:
	docker compose down
	docker compose up --build

ci-k6-test: start-payment-processors start-backend k6-test

build-image:
	docker build . -t distanteagle16/rinhabackend3

push-image:
	docker push distanteagle16/rinhabackend3