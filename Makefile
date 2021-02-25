NAME = server${NUMBER}

.PHONY: build

build:
	@docker build -t cpen431 --target bin .
run:
	@docker run -it --rm -p 8000:3000 \
	--memory=128m --memory-swap=128m \
	--name ${NAME} \
	cpen431 ${PORT}