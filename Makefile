.DEFAULT_GOAL := all

.PHONY: up-all down-all

all:
	make down-all
	make up-all

up-all:
	cd docker && docker-compose up -d

down-all:
	cd docker && docker-compose down

up-%:
	cd docker && docker-compose up -d $*

stop-%:
	cd docker && docker-compose stop $*

logs-%:
	cd docker && docker-compose logs -f $*
