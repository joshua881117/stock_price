GIT_TAG := $(shell git describe --abbrev=0 --tags)
# test-api:
# 	pipenv run pytest --cov-report term-missing --cov-config=.coveragerc --cov=./api/ tests/

build-image:
	docker build -f Dockerfile -t joshua881117/extending_airflow:${GIT_TAG} .

push-image:
	docker push joshua881117/extending_airflow:${GIT_TAG}

# deploy-airflow:
# 	GIT_TAG=${GIT_TAG} docker stack deploy --with-registry-auth -c api.yml api

