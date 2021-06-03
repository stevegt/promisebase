package pit

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/opencontainers/runtime-tools/generate"

	. "github.com/stevegt/goadapt"
)

type Container struct {
	Image string
	Args  []string
	Cid   string
	Name  string
	Rc    int
	Errc  chan error
	dir   string
	*exec.Cmd
}

// XXX only handles docker images, cant handle trees yet
func (pit *Pit) startContainer(cntr *Container) (err error) {
	defer Return(&err)

	err = cntr.initdir()
	Ck(err)

	err = cntr.initconfig()
	Ck(err)

	err = cntr.createimg()
	Ck(err)

	err = cntr.createrootfs()
	Ck(err)

	err = cntr.start()
	Ck(err)

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

func (cntr *Container) initdir() (err error) {
	defer Return(&err)

	// XXX correct dir?
	dir, err := ioutil.TempDir("", "pitd")
	Ck(err)
	// log.Debugf(os.Stderr, "bundle dir: %s\n", dir)
	err = os.Chdir(dir)
	Ck(err)

	cntr.dir = dir

	return
}

func (cntr *Container) initconfig() (err error) {
	defer Return(&err)

	err = os.Chdir(cntr.dir)
	Ck(err)

	// create config file and set permissions
	config, err := os.OpenFile("config.json", os.O_RDWR|os.O_CREATE, 0755)
	Ck(err)

	spec, err := generate.New("linux")

	var exportOpts generate.ExportOptions
	// exportOpts.Seccomp = true

	spec.SetProcessTerminal(true)
	spec.SetProcessArgs(cntr.Args)

	//write to config.json
	err = spec.Save(config, exportOpts)
	Ck(err)

	return
}

func (cntr *Container) createimg() (err error) {
	defer Return(&err)

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

	return
}

func (cntr *Container) createrootfs() (err error) {
	defer Return(&err)

	err = os.Chdir(cntr.dir)
	Ck(err)

	err = os.MkdirAll("rootfs", 0755)
	Ck(err)

	export := exec.Command("docker", "export", cntr.Cid)
	// export.Stderr = os.Stderr
	export.Stderr = nil

	tar := exec.Command("tar", "-C", "rootfs", "-xvf", "-")
	// tar.Stdout = os.Stdout
	tar.Stderr = os.Stderr
	tar.Stdout = nil
	// tar.Stderr = nil

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

	return
}

func (cntr *Container) createRootFsFromTree() (err error) {
	defer Return(&err)

	err = os.Chdir(cntr.dir)
	Ck(err)

	// XXX
	return
}

func (cntr *Container) start() (err error) {
	defer Return(&err)

	fmt.Fprintf(os.Stderr, "starting container\n")
	if cntr.Name == "" {
		_, cntr.Name = filepath.Split(cntr.dir)
	}
	cntr.Cmd.Path = "/usr/bin/sudo"
	cntr.Cmd.Args = []string{"sudo", "runc", "run", cntr.Name}
	err = cntr.Start()
	Ck(err)
	fmt.Println("container started")

	return
}
