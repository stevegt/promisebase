test:
	go test -v
	./covertest.sh
	golint -set_exit_status

