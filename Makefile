.PHONY: help test

# required by the docker-compose.yml
export EXPOSED_PORT?=8081

help:
# http://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

build: ## Make all docker images locally
	docker-compose build

dev: build ## Make a dev flask server
	docker-compose run \
		--rm \
		-e FLASK_APP=yarnitor \
		-e FLASK_DEBUG=1 \
		-p $(EXPOSED_PORT):5000 \
		web \
		flask run -h 0.0.0.0

down: ## Make all docker containers stop
	docker-compose down

mock: ## Make all docker containers start pointing at a mock YARN service
	docker-compose -f docker-compose.test.yml up --build

prod: build ## Make all docker containers start pointing at YARN_ENDPOINT
	docker-compose up

test: ## Make a pytest run
	docker-compose -f docker-compose.test.yml up -d --build
	sleep 5
	docker-compose -f docker-compose.test.yml run --rm test pytest; \
		TEST_STATUS=$$?; \
		docker-compose -f docker-compose.test.yml down; \
		exit "$$TEST_STATUS"