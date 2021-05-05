module github.com/t7a/pitbase/cmd/pb

go 1.14

replace github.com/t7a/pitbase/db => ../../db

// replace github.com/stevegt/goadapt => /home/stevegt/lab/goadapt

require (
	github.com/Microsoft/go-winio v0.4.18 // indirect
	github.com/containerd/containerd v1.4.4 // indirect
	github.com/docker/distribution v2.7.1+incompatible // indirect
	github.com/docker/docker v20.10.6+incompatible
	github.com/docker/go-connections v0.4.0 // indirect
	github.com/docker/go-units v0.4.0 // indirect
	github.com/docopt/docopt-go v0.0.0-20180111231733-ee0de3bc6815
	github.com/gogo/protobuf v1.1.1 // indirect
	github.com/google/go-cmdtest v0.3.0
	github.com/gorilla/mux v1.8.0 // indirect
	github.com/kr/pretty v0.1.0 // indirect
	github.com/moby/term v0.0.0-20201216013528-df9cb8a40635 // indirect
	github.com/morikuni/aec v1.0.0 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.0.1 // indirect
	github.com/pkg/fileutils v0.0.0-20181114200823-d734b7f202ba
	github.com/sirupsen/logrus v1.7.0
	github.com/stevegt/debugpipe v0.0.2
	github.com/stevegt/goadapt v0.0.9
	github.com/t7a/pitbase/db v0.0.0-00010101000000-000000000000
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4 // indirect
	google.golang.org/grpc v1.37.0 // indirect
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127 // indirect
	gopkg.in/yaml.v2 v2.3.0 // indirect
	gotest.tools/v3 v3.0.3 // indirect
)
