test:
	rm -rf var
	# go test -v -timeout 10s
	./covertest.sh
	golint -set_exit_status

