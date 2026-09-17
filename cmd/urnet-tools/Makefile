
.PHONY: all clean build

# all depends on clean so cleanup always completes before compilation;
# without .PHONY, a non-phony build target can be skipped when build/ exists
# (CodeRabbit).
all: build

clean:
	rm -rf build

build: clean
	env CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o build/linux/arm64/ .
	env CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o build/linux/arm64/ ../urnetwork-updater
	env CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o build/linux/amd64/ .
	env CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o build/linux/amd64/ ../urnetwork-updater
	env CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -o build/darwin/arm64/ .
	env CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -o build/darwin/arm64/ ../urnetwork-updater
	env CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -o build/darwin/amd64/ .
	env CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -o build/darwin/amd64/ ../urnetwork-updater
	env CGO_ENABLED=0 GOOS=windows GOARCH=arm64 go build -o build/windows/arm64/ .
	env CGO_ENABLED=0 GOOS=windows GOARCH=arm64 go build -o build/windows/arm64/ ../urnetwork-updater
	env CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o build/windows/amd64/ .
	env CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o build/windows/amd64/ ../urnetwork-updater
