REPO = github.com/imega/mt
CWD = /go/src/githib.com/$(REPO)
GO_IMG = golang:stretch

test: lint clean
	@GO_IMG=$(GO_IMG) CWD=$(CWD) docker-compose up --abort-on-container-exit

lint:
	@docker run --rm -t -v $(CURDIR):$(CWD) -w $(CWD) \
		golangci/golangci-lint golangci-lint run

clean:
	@TAG=$(TAG) docker-compose down -v --remove-orphans
