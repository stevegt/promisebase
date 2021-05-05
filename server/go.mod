module github.com/t7a/pitbase/server

go 1.14

replace github.com/t7a/pitbase/db => ../db

// replace github.com/stevegt/goadapt => /home/stevegt/lab/goadapt
// replace github.com/stevegt/readercomp => /home/stevegt/lab/readercomp

require (
	github.com/Microsoft/go-winio v0.5.0 // indirect
	github.com/containerd/containerd v1.4.4 // indirect
	github.com/docker/distribution v2.7.1+incompatible // indirect
	github.com/docker/docker v20.10.6+incompatible
	github.com/docker/go-connections v0.4.0 // indirect
	github.com/docker/go-units v0.4.0 // indirect
	github.com/fsnotify/fsnotify v1.4.9
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/google/go-cmp v0.5.5 // indirect
	github.com/gorilla/mux v1.8.0 // indirect
	github.com/moby/term v0.0.0-20201216013528-df9cb8a40635 // indirect
	github.com/morikuni/aec v1.0.0 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.0.1 // indirect
	github.com/stevegt/debugpipe v0.0.2
	github.com/stevegt/goadapt v0.0.9
	github.com/stevegt/readercomp v0.0.1
	github.com/t7a/pitbase/db v0.0.0-00010101000000-000000000000
	golang.org/x/time v0.0.0-20210220033141-f8bda1e9f3ba // indirect
	google.golang.org/grpc v1.37.0 // indirect
	gotest.tools/v3 v3.0.3 // indirect
)
