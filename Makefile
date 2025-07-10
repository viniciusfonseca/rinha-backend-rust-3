k6-test:
	k6 run tests/rinha.js

image:
	docker build . -t distanteagle16/rinhabackend3

push-image:
	docker push distanteagle16/rinhabackend3