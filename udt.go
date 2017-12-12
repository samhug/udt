package udt

import (
	"fmt"
	"io"
	"io/ioutil"
	"regexp"
	"strconv"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

func NewConnection(client *ssh.Client, udtBin string, udtHome string) *Connection {
	c := &Connection{
		udtBin:    udtBin,
		udtHome:   udtHome,
		sshClient: client,
	}

	return c
}

type Connection struct {
	udtBin    string
	udtHome   string
	sshClient *ssh.Client
}

type PhantomProc struct {
	Pid     int
	OutFile string
}

// Execute runs the provided unidata command as a PHANTOM process
func (c *Connection) ExecutePhantom(cmd string) (*PhantomProc, error) {

	// Open a new SSH session
	session, err := c.sshClient.NewSession()
	if err != nil {
		return nil, fmt.Errorf("unable to create SSH session: %s", err)
	}
	defer session.Close()

	// Get an io.Reader for stderr
	outputPipe, err := session.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("unable to attach to SSH stdout pipe: %s", err)
	}

	fullCmd := fmt.Sprintf("UDTHOME=%s;UDTBIN=%s; cd $UDTHOME; $UDTBIN/udt PHANTOM %s",
		strconv.Quote(c.udtHome),
		strconv.Quote(c.udtBin),
		strconv.Quote(cmd),
	)
	if err := session.Run(fullCmd); err != nil {
		return nil, fmt.Errorf("udt execute failed: %q", err)
	}

	// Collect the output
	outputBuf, err := ioutil.ReadAll(outputPipe)
	if err != nil {
		return nil, fmt.Errorf("unable to read UDT output: %s", err)
	}
	output := string(outputBuf)

	// We're expecting output of the form:
	//
	// PHANTOM process ######## started.
	// COMO file is '_PH_/user#####_#######'.
	//
	re := regexp.MustCompile("PHANTOM process (\\d+) started\\.\nCOMO file is '(.+)'\\.")
	match := re.FindStringSubmatch(output)
	if match == nil || len(match) != 3 {
		return nil, fmt.Errorf("error parsing output:\n%s\n%s", output, err)
	}
	pid, err := strconv.Atoi(match[1])
	if err != nil {
		panic("Surely a sign of the end times...")
	}

	return &PhantomProc{
		Pid:     pid,
		OutFile: fmt.Sprintf("%s/%s", c.udtHome, match[2]),
	}, nil
}

// Wait will block until the specified PHANTOM process terminates
func (c *Connection) Wait(proc *PhantomProc) error {
	// Open a new SSH session
	session, err := c.sshClient.NewSession()
	if err != nil {
		return fmt.Errorf("unable to create SSH session: %s", err)
	}
	defer session.Close()

	if err := session.Run(fmt.Sprintf("/usr/bin/wait %d", proc.Pid)); err != nil {
		// If the error is an exit status of 127, ignore it. That just means the PID wasn't found
		if serr, ok := err.(*ssh.ExitError); !ok || serr.ExitStatus() != 127 {
			return fmt.Errorf("error waiting for process to terminate: %s", err)
		}
	}

	return nil
}

// RetreiveOutput
func (c *Connection) RetreiveOutput(proc *PhantomProc) (io.ReadCloser, error) {

	// Initiate SFTP session
	session, err := sftp.NewClient(c.sshClient)
	if err != nil {
		return nil, fmt.Errorf("unable to initiate SFTP session: %s", err)
	}

	// Retrieve COMO file and verify the command ran successfuly
	f, err := session.Open(proc.OutFile)
	if err != nil {
		return nil, fmt.Errorf("unable to open UDT output file (%s): %s", proc.OutFile, err)
	}

	return newHookedCloser(f, func() error {
		defer session.Close()
		defer f.Close()
		return nil
	}), nil
}

type hookedCloser struct {
	io.Reader
	closer func() error
}

func (r *hookedCloser) Close() error { return r.closer() }

func newHookedCloser(r io.Reader, closer func() error) io.ReadCloser {
	return &hookedCloser{r, closer}
}
