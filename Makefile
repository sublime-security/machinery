.PHONY: fmt lint golint test ci
# TODO: When Go 1.9 is released vendor folder should be ignored automatically
PACKAGES=`go list ./... | grep -v vendor | grep -v mocks`

fmt:
	for pkg in ${PACKAGES}; do \
		go fmt $$pkg; \
	done;

lint:
	gometalinter --tests --disable-all --deadline=120s -E vet -E gofmt -E misspell -E ineffassign -E goimports -E deadcode ./...

golint:
	for pkg in ${PACKAGES}; do \
		golint -set_exit_status $$pkg || GOLINT_FAILED=1; \
	done; \
	[ -z "$$GOLINT_FAILED" ]

test:
	go version
	TEST_FAILED= ; \
	for pkg in ${PACKAGES}; do \
	  	echo "$$pkg"; \
		go test -v -timeout 20m -buildvcs=false $$pkg || TEST_FAILED=1; \
		echo "TEST_FAILED=$$TEST_FAILED"; \
	done; \
	[ -z "$$TEST_FAILED" ]

ci:
	bash -c 'docker compose -f docker-compose.test.yml -p machinery_ci up --build --abort-on-container-exit --exit-code-from sut'
