test:
	rm -rf var
	# go test -v -timeout 10s
	./covertest.sh


pprof:
	go test -cpuprofile /tmp/pitbase.prof -bench=.
	go tool pprof -svg /tmp/pitbase.prof > /tmp/pitbase.prof.svg
	go tool pprof /tmp/pitbase.prof

pprof-server:
	go test -cpuprofile /tmp/pitbase.prof -bench=.
	go tool pprof -http=":5910" /tmp/pitbase.prof

bench:
	go test -bench=.
