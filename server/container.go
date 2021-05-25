package pit

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
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
	Image string
	Args  []string
	Cid   string
	Name  string
	Rc    int
	Errc  chan error
	*exec.Cmd
}

// XXX only handles docker images, cant handle trees yet
func (pit *Pit) startContainer(cntr *Container) (err error) {
	defer Return(&err)

	// XXX correct dir?
	dir, err := ioutil.TempDir("", "pitd")
	Ck(err)
	// log.Debugf(os.Stderr, "bundle dir: %s\n", dir)
	err = os.Chdir(dir)
	Ck(err)

	if cntr.Name == "" {
		_, cntr.Name = filepath.Split(dir)
	}

	// generate args that will go into config.json
	cnfg := &exec.Cmd{
		// XXX quick workaround because oci-runtime-tool is not in the path for some reason
		Path:   filepath.Join(os.Getenv("HOME"), "/.goenv/shims/oci-runtime-tool"),
		Args:   []string{"oci-runtime-tool", "generate", "--process-terminal"},
		Stderr: os.Stderr,
	}
	for _, s := range cntr.Args {
		cnfg.Args = append(cnfg.Args, "--args", s)
	}
	stdout, err := cnfg.StdoutPipe()
	Ck(err)
	fmt.Printf("config args: %v\n", cnfg.Args)

	// create config file and set permissions
	configw, err := os.OpenFile("config.json", os.O_RDWR|os.O_CREATE, 0755)
	Ck(err)

	// start config
	fmt.Printf("PATH=%s\n", os.Getenv("PATH"))
	err = cnfg.Start()
	Ck(err)
	_, err = io.Copy(configw, stdout)
	Ck(err)
	err = cnfg.Wait()
	Ck(err)
	err = configw.Close()
	Ck(err)

	err = os.MkdirAll("rootfs", 0755)
	Ck(err)

	// create docker image
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
	cntr.Cmd.Path = "/usr/bin/sudo"
	cntr.Cmd.Args = []string{"sudo", "runc", "run", cntr.Name}
	err = cntr.Start()
	Ck(err)
	fmt.Println("container started")

	return
}

func (cntr *Container) Delete() (err error) {
	// XXX check to see if container is already gone
	runc := exec.Command("sudo", "runc", "delete", cntr.Name)
	// XXX log?
	runc.Stdout = os.Stdout
	runc.Stderr = os.Stderr
	err = runc.Start()
	Ck(err)
	err = runc.Wait()
	Ck(err)
	// XXX remove bundle dir
	return
}

func (cntr *Container) Wait() (err error) {
	err = cntr.Cmd.Wait()
	cntr.Rc = cntr.Cmd.ProcessState.ExitCode()
	return
}
