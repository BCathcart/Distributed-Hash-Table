all: bin/server

PLATFORM=local

.PHONY: bin/server
bin/server:
	@docker build . --target bin \
	--output bin/ \
	--platform ${PLATFORM} \
	--memory 128m \
	--memory-swap 128m