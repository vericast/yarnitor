.PHONY: help

# required by the docker-compose.yml
export EXPOSED_PORT:=8081

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
		-p 5000:5000 \
		web \
		flask run -h 0.0.0.0

down: ## Make all docker containers stop
	docker-compose down

prod: build ## Make a production flask server behind gunicorn
	docker-compose up

test: build ## Make a pytest run
	docker-compose run --rm web pytest
