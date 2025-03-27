
build:
	mkdir -p ./dist
	go build -o dist/s3tonowhere -tags osusergo,netgo -ldflags="-s -w"

format:
	go fmt
