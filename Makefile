GIT_TAG := $(shell git describe --abbrev=0 --tags) # 獲取 git repository 最新的版本號

build-image:
	docker build -f Dockerfile -t joshua881117/extending_airflow:${GIT_TAG} .

push-image:
	docker push joshua881117/extending_airflow:${GIT_TAG}

deploy-airflow:
	GIT_TAG=${GIT_TAG} docker compose up -d


