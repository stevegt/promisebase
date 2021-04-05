test:
	rm -rf var
	# go test -v -timeout 10s
	./covertest.sh

testloop:
	while true; do inotifywait -e move *.go cmd/pb/*.go cmd/pb/testdata/main.ct; sleep 1; make test; done

pprof:
	go test -cpuprofile /tmp/pitbase.prof -bench=.
	go tool pprof -svg /tmp/pitbase.prof > /tmp/pitbase.prof.svg
	go tool pprof /tmp/pitbase.prof

pprof-server:
	go test -cpuprofile /tmp/pitbase.prof -bench=.
	go tool pprof -http=":5910" /tmp/pitbase.prof

bench:
	go test -bench=.
