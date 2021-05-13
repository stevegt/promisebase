package pit

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/cio"
	"github.com/containerd/containerd/namespaces"
	"github.com/containerd/containerd/oci"

	. "github.com/stevegt/goadapt"
)

func (pit *Pit) runContainer(outstream, errstream io.WriteCloser, img string, cmd ...string) (rc int, err error) {
	defer Return(&err)

	// create a new client connected to the default socket path for containerd
	// fn := "/run/docker/containerd/docker-containerd.sock"
	fn := "/run/containerd/containerd.sock"
	client, err := containerd.New(fn)
	Ck(err)
	defer client.Close()

	// create a new context with a "pit" namespace
	ctx := namespaces.WithNamespace(context.Background(), "pit")

	var image containerd.Image
	if strings.Index(img, "tree/") == 0 {
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
		fmt.Println("pulling")
		image, err = client.Pull(ctx, img, containerd.WithPullUnpack)
		Ck(err)
		fmt.Println("pull done")
	}

	// create a container
	name := "test-13" // XXX get a short name
	container, err := client.NewContainer(
		ctx,
		name,
		containerd.WithImage(image),
		containerd.WithNewSnapshot(name+"-snapshot", image),
		containerd.WithNewSpec(oci.WithImageConfigArgs(image, cmd)),
	)
	Ck(err)
	// XXX we do want to delete, right?
	defer container.Delete(ctx, containerd.WithSnapshotCleanup)

	// create a task from the container
	// XXX do something with stdin
	streams := cio.WithStreams(os.Stdin, outstream, errstream)
	// streams := cio.WithStreams(os.Stdin, os.Stdout, os.Stderr)
	task, err := container.NewTask(ctx, cio.NewCreator(streams))
	Ck(err)
	defer task.Delete(ctx)

	fmt.Println("container created")
	// make sure we wait before calling start
	// XXX why?
	exitStatusC, err := task.Wait(ctx)
	_ = exitStatusC
	if err != nil {
		// XXX why not abend?
		fmt.Println(err)
	}

	// call start on the task to execute the redis server
	err = task.Start(ctx)
	Ck(err)

	fmt.Println("container task started")
	// sleep for a lil bit to see the logs
	// XXX get rid of sleep
	time.Sleep(1 * time.Second)

	// kill the process and get the exit status
	// XXX no
	fmt.Println("killing container task")
	// err = task.Kill(ctx, syscall.SIGTERM)
	err = task.Kill(ctx, syscall.SIGKILL)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("container task killed")
	// wait for the process to fully exit and print out the exit status

	// status := <-exitStatusC
	// fmt.Println("got status")
	// code, _, err := status.Result()
	// Ck(err)
	// XXX
	// fmt.Printf("exited with status: %d\n", code)
	fmt.Println("exiting with no status")

	return
}
