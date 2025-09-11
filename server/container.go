package pit

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/codeclysm/extract/v3"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/opencontainers/runtime-tools/generate"

	// "github.com/opencontainers/image-tools/image"
	// "github.com/opencontainers/runtime-tools/generate"

	pb "github.com/t7a/pitbase/db"

	. "github.com/stevegt/goadapt"
)

type Container struct {
	Path string
	Args []string
	Cid  string
	Name string
	Rc   int
	Errc chan error
	dir  string
	pit  *Pit
	*exec.Cmd
}

// startContainer starts a container from a db tree
func (pit *Pit) startContainer(cntr *Container) (err error) {
	defer Return(&err)

	err = cntr.initdir()
	Ck(err, "initdir failed")

	err = cntr.initconfig()
	Ck(err, "initconfig failed")

	err = cntr.createRootFsFromTree()
	Ck(err, "createrootfs failed")

	err = cntr.start()
	Ck(err, "start failed")

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
	// XXX use docker API instead of CLI
	create := exec.Command("docker", "create", cntr.Path)
	out, err := create.CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker create failed: %v:\n %s", err, out)
	}

	cntr.Cid = strings.TrimSpace(string(out))
	fmt.Fprintf(os.Stderr, "container id: %q\n", cntr.Cid)

	return
}

// creatrootfsFromDocker creates a root filesystem in the rootfs directory of the
// current working directory by exporting the docker image and untarring it
// XXX deprecate
func (cntr *Container) createRootfsFromDockerInage() (err error) {
	defer Return(&err)

	err = os.Chdir(cntr.dir)
	Ck(err)

	err = os.MkdirAll("rootfs", 0755)
	Ck(err)

	// export docker image
	export := exec.Command("docker", "export", cntr.Cid)
	// export.Stderr = os.Stderr
	export.Stderr = nil

	// untar to rootfs
	// XXX use archive/tar instead of CLI
	tar := exec.Command("tar", "-C", "rootfs", "-xvf", "-")
	// tar.Stdout = os.Stdout
	tar.Stderr = os.Stderr
	tar.Stdout = nil
	// tar.Stderr = nil

	// pipe the export output to the tar input
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

// createrootfs creates a runc-compatible root filesystem in the rootfs
// subdirectory of the current working directory by unpacking the image tree
// from the database.  An image tree is an oci archive that has been
// stored in the database.
func (cntr *Container) createRootFsFromTree() (err error) {
	defer Return(&err)

	err = os.Chdir(cntr.dir)
	Ck(err)

	err = os.MkdirAll("rootfs", 0755)
	Ck(err)

	path, err := pb.Path{}.New(cntr.pit.Db, cntr.Path)
	Ck(err)
	// tree is an io.Reader containing an oci archive of the image
	tree, err := cntr.pit.Db.GetTree(path)
	Ck(err)

	// unpack the oci archive to a temporary directory for debugging
	// XXX remove
	err = extract.Tar(context.Background(), tree, "/tmp/tdb7", nil)
	Ck(err)
	tree.Rewind()

	// convert the oci archive to a tar stream
	var tarRd io.ReadCloser
	tarRd, err = oci2tar(tree, "rootfs")
	Ck(err)

	// unpack the tar stream to the rootfs directory
	err = extract.Tar(context.Background(), tarRd, "rootfs", nil)
	Ck(err)
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

// ImportImage imports a container image into the database and returns
// a tree.  The image name must be a docker image name, e.g.
// "docker.io/library/ubuntu:latest".  The image is stored in the database
// as an OCI archive.
func (pit *Pit) ImportImage(algo, img string) (tree *pb.Tree, err error) {
	tmpfile, err := ioutil.TempFile("", "*.oci")
	Ck(err)
	path := tmpfile.Name()
	defer os.Remove(path)
	cmd := exec.Command("skopeo", "copy", img, fmt.Sprintf("oci-archive:%s", path))
	fmt.Println(cmd.Args)
	// fmt.Println(tmpfile.Name(), dest)
	err = cmd.Run()
	Ck(err)
	imgrd, err := os.Open(path)
	Ck(err)
	tree, err = pit.Db.PutStream(algo, imgrd)
	Ck(err)

	/*
		ctx := context.Background()
		cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())

		// pull container image
		pullrd, err := cli.ImagePull(ctx, img, types.ImagePullOptions{})
		if err != nil {
			panic(err)
		}
		io.Copy(os.Stdout, pullrd)

		// save image as a stream
		saverd, err := cli.ImageSave(ctx, []string{img})
		tree, err = pit.Db.PutStream(algo, saverd)
	*/
	return
}

// oci2tar extracts the root filesystem from an OCI image tarball
// provided as an io.Reader and returns it as a tar stream io.Reader.
func oci2tar(reader io.Reader, rootfsPath string) (tar io.ReadCloser, err error) {
	// Convert io.Reader to io.ReadCloser
	var rc io.ReadCloser
	rc, ok := reader.(io.ReadCloser)
	Assert(ok, "reader is not an io.ReadCloser")

	// Create an Opener function that returns your ReadCloser
	opener := func() (io.ReadCloser, error) {
		return rc, nil
	}

	// show the tarball contents for debugging

	// Load the OCI image from the tarball using the Opener
	img, err := tarball.Image(opener, nil)
	Ck(err)

	// Extract the flattened filesystem into a tar stream
	rootfsReader := mutate.Extract(img)
	defer rootfsReader.Close()

	// Return the tar stream reader
	return rootfsReader, nil
}

// XXX remove
func extractTarToDir(tarReader io.ReadCloser, destDir string) error {
	tr := tar.NewReader(tarReader)

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		target := filepath.Join(destDir, header.Name)

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0755); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return err
			}

			file, err := os.Create(target)
			if err != nil {
				return err
			}

			_, err = io.Copy(file, tr)
			file.Close()
			if err != nil {
				return err
			}
		}
	}
	return nil
}
