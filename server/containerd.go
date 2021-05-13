package pit

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/cio"
	"github.com/containerd/containerd/namespaces"
	"github.com/containerd/containerd/oci"
	"github.com/docker/docker/pkg/namesgenerator"

	. "github.com/stevegt/goadapt"
)

type ContainerRuntime struct {
	fn     string
	client *containerd.Client
}

// create a new client connected to the default socket path for containerd
func (pit *Pit) connectRuntime(fn string) (err error) {
	defer Return(&err)
	Assert(pit.runtime == nil)
	client, err := containerd.New(fn)
	Ck(err)
	runtime := &ContainerRuntime{
		fn:     fn,
		client: client,
	}
	pit.runtime = runtime
	return
}

type Container struct {
	Image  string
	Args   []string
	Stdin  io.ReadCloser
	Stdout io.WriteCloser
	Stderr io.WriteCloser
	task   containerd.Task
	ctx    context.Context
	rc     int
	err    error
}

func (pit *Pit) startContainer(cntr *Container) (err chan error) {
	defer Return(&err)

	// create a new context with a "pit" namespace
	cntr.ctx = namespaces.WithNamespace(context.Background(), "pit")

	client := pit.runtime.client

	var image containerd.Image
	if strings.Index(cntr.Image, "tree/") == 0 {
		// XXX convert to containerd API
		/*
			path := pb.Path{}.New(pit.Db, img)
			tree, err := pit.Db.GetTree(path)
			Ck(err)
			defer tree.Close()

			var res types.ImageLoadResponse
			if true {
				res, err = cli.ImageLoad(ctx, tree, false)
				Ck(err)
			} else {
				pipeReader, pipeWriter := debugpipe.Pipe()
				go func() {
					_, err = io.Copy(pipeWriter, tree)
					Ck(err)
					err = pipeWriter.Close()
					Ck(err)
				}()
				res, err = cli.ImageLoad(ctx, pipeReader, false)
				Ck(err)
			}

			_, err = io.Copy(os.Stdout, res.Body)
			Ck(err)
			defer res.Body.Close()
		*/
	} else {
		// pull the image from DockerHub
		// XXX always pull into a tree, run from there
		fmt.Println("pulling")
		image, err = client.Pull(cntr.ctx, cntr.Image, containerd.WithPullUnpack)
		Ck(err)
		fmt.Println("pull done")
	}

	// create a container
	var container containerd.Container
	for i := 0; i < 10; i++ {
		// generate name
		// XXX allow name to be passed in instead
		name := namesgenerator.GetRandomName(i)
		container, err = client.NewContainer(
			cntr.ctx,
			name,
			containerd.WithImage(image),
			// XXX delete or re-use existing snapshot?
			// XXX use WithSnapshot for tree-based containers
			containerd.WithNewSnapshot(name+"-snapshot", image),
			// XXX deal with existing spec
			containerd.WithNewSpec(oci.WithImageConfigArgs(image, cntr.Args)),
		)
		if err != nil {
			// likely name collision -- retry
			// XXX actually look at the err instead of blindly
			// retrying
			continue
		}
	}
	Ck(err)

	// create a task from the container
	streams := cio.WithStreams(cntr.Stdin, cntr.Stdout, cntr.Stderr)
	cntr.task, err = container.NewTask(cntr.ctx, cio.NewCreator(streams))
	Ck(err)

	// call start on the task
	err = cntr.task.Start(cntr.ctx)
	Ck(err)

	fmt.Println("container task started")

	return
}

func (cntr *Container) Delete() {
	defer cntr.task.Delete(cntr.ctx)
}
