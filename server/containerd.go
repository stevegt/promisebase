package pit

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"

	"github.com/containerd/containerd"

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
	Cid    string
	Name   string
	Stdin  io.ReadCloser
	Stdout io.WriteCloser
	Stderr io.WriteCloser
	rc     int
	Errc   chan error
}

func (pit *Pit) startContainer(cntr *Container) (err error) {
	defer Return(&err)

	// XXX correct dir?
	err = os.Chdir(pit.Dir)
	Ck(err)

	spec := exec.Command("runc", "spec")
	err = spec.Start()
	Ck(err)
	err = spec.Wait()
	Ck(err)

	create := exec.Command("docker", "create", cntr.Image)
	createOut, err := create.StdoutPipe()
	Ck(err)
	err = create.Start()
	Ck(err)
	containerId, err := ioutil.ReadAll(createOut)
	Ck(err)
	err = create.Wait()
	Ck(err)
	cntr.Cid = strings.TrimSpace(string(containerId))
	fmt.Fprintf(os.Stderr, "container id: %q\n", cntr.Cid)

	export := exec.Command("docker", "export", cntr.Cid)
	export.Stderr = os.Stderr

	tar := exec.Command("tar", "-C", "rootfs", "-xvf", "-")
	tar.Stdout = os.Stdout
	tar.Stderr = os.Stderr

	tarpipe, err := tar.StdinPipe()
	Ck(err)
	export.Stdout = tarpipe

	fmt.Fprintf(os.Stderr, "starting tar\n")
	err = tar.Start()
	Ck(err)

	fmt.Fprintf(os.Stderr, "starting export\n")
	err = export.Start()
	Ck(err)

	fmt.Fprintf(os.Stderr, "export waiting\n")
	err = export.Wait()
	if err != nil {
		fmt.Fprintf(os.Stderr, "export: %v\n", err)
	}

	tarpipe.Close()

	fmt.Fprintf(os.Stderr, "tar waiting\n")
	err = tar.Wait()
	if err != nil {
		fmt.Fprintf(os.Stderr, "tar: %v\n", err)
	}

	fmt.Fprintf(os.Stderr, "starting container\n")
	// XXX what should the container be named instead of foo?
	runc := exec.Command("sudo", "runc", "run", cntr.Name)
	runc.Stdin = os.Stdin
	runc.Stdout = os.Stdout
	runc.Stderr = os.Stderr
	err = runc.Start()
	Ck(err)
	err = runc.Wait()
	Ck(err)

	_ = runc.ProcessState.ExitCode()
	fmt.Println("container started")

	return
}

func (cntr *Container) Delete() (err error) {
	runc := exec.Command("sudo", "runc", "delete", cntr.Name)
	err = runc.Start()
	Ck(err)
	err = runc.Wait()
	Ck(err)
	return
}
