package udt

import (
	"fmt"
	"io"
	"io/ioutil"
	"regexp"
	"strconv"
	"strings"

	"github.com/pkg/sftp"
	"github.com/samuelhug/udt/truncatereader"
	"golang.org/x/crypto/ssh"
)

// EnvConfig holds configuration info for a UDT client
type EnvConfig struct {
	UdtBin  string
	UdtHome string
	UdtAcct string
}

// NewClient creates a udt.Client object from the provided SSH client
func NewClient(client *ssh.Client, env *EnvConfig) *Client {

	if env.UdtAcct == "" {
		panic("udt.NewClient: env.UdtAcct must not be blank")
	}
	if env.UdtBin == "" {
		panic("udt.NewClient: env.UdtBin must not be blank")
	}
	if env.UdtHome == "" {
		panic("udt.NewClient: env.UdtHome must not be blank")
	}

	c := &Client{
		env:       env,
		sshClient: client,
	}

	return c
}

// Client represents a Unidata database client
type Client struct {
	env       *EnvConfig
	sshClient *ssh.Client
}

// PhantomProc represents a PHANTOM process running on the database
type PhantomProc struct {
	Pid     int
	OutFile string
}

// ExecutePhantomAsync runs the provided unidata command as a PHANTOM process
func (c *Client) ExecutePhantomAsync(cmd string) (_ *PhantomProc, err error) {

	// Open a new SSH session
	session, err := c.sshClient.NewSession()
	if err != nil {
		return nil, fmt.Errorf("failed to create SSH session: %s", err)
	}
	defer safeCloseIgnoreEOF(session, "failed to close SSH session", &err)

	// Get an io.Reader for stderr
	outputPipe, err := session.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to attach to SSH stderr pipe: %s", err)
	}

	// TODO: Fix shell escaping here, strconv.Quote is for escaping Go string literals not shell commands
	shellCmd := fmt.Sprintf("UDTHOME=%s;UDTBIN=%s; cd %s; $UDTBIN/udt PHANTOM %s",
		strconv.Quote(c.env.UdtHome),
		strconv.Quote(c.env.UdtBin),
		strconv.Quote(c.env.UdtAcct),
		strconv.Quote(cmd),
	)
	if err := session.Run(shellCmd); err != nil {
		outputBuf, _ := ioutil.ReadAll(outputPipe)
		return nil, fmt.Errorf("udt execute failed for command '%s'\n===\n%s\n===\n%s", shellCmd, outputBuf, err)
	}

	// Collect the output
	outputBuf, err := ioutil.ReadAll(outputPipe)
	if err != nil {
		return nil, fmt.Errorf("failed to read UDT stderr output: %s", err)
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
		return nil, fmt.Errorf("error parsing UDT stderr output:\n===\n%s\n===\n%s", output, err)
	}
	pid, err := strconv.Atoi(match[1])
	if err != nil {
		panic("ExecutePhantomAsync: surely a sign of the end times...")
	}

	return &PhantomProc{
		Pid:     pid,
		OutFile: fmt.Sprintf("%s/%s", c.env.UdtAcct, match[2]),
	}, nil
}

// Wait will block until the specified PHANTOM process terminates
func (c *Client) Wait(proc *PhantomProc) (err error) {

	// Open a new SSH session
	session, err := c.sshClient.NewSession()
	if err != nil {
		return fmt.Errorf("failed to create SSH session: %s", err)
	}
	defer safeCloseIgnoreEOF(session, "failed to close SSH session", &err)

	if err := session.Run(fmt.Sprintf("/usr/bin/wait %d", proc.Pid)); err != nil {
		// If the error is an exit status of 127, ignore it. That just means the PID wasn't found
		if err, ok := err.(*ssh.ExitError); !ok || err.ExitStatus() != 127 {
			return fmt.Errorf("error waiting for process to terminate: %s", err)
		}
	}

	return nil
}

// ExecutePhantom runs the provided unidata command as a PHANTOM process, waits for it to complete, and returns a reader with output
func (c *Client) ExecutePhantom(cmd string) (io.ReadCloser, error) {

	proc, err := c.ExecutePhantomAsync(cmd)
	if err != nil {
		return nil, fmt.Errorf("ExecutePhantomAsync failed: %s", err)
	}

	if err := c.Wait(proc); err != nil {
		return nil, fmt.Errorf("Wait failed: %s", err)
	}

	r, err := c.RetrieveOutput(proc)
	if err != nil {
		return nil, fmt.Errorf("RetrieveOutput failed: %s", err)
	}

	return r, nil
}

// CompileBasicProgram uploads BASIC source code to the UDT server and compiles it
func (c *Client) CompileBasicProgram(progFile string, progName string, progSrc string) (err error) {

	//TODO: Assert that progFile is a directory that already exists
	//TODO: Assert that the target file doesn't already exist

	if progFile == "" {
		return fmt.Errorf("progFile must not be blank")
	}
	if progName == "" {
		return fmt.Errorf("progName must not be blank")
	}

	// Initialize SFTP client
	client, err := sftp.NewClient(c.sshClient)
	if err != nil {
		return fmt.Errorf("failed to initialize SFTP client: %s", err)
	}
	defer safeClose(client, "failed to close SFTP client", &err)

	srcPath := c.env.UdtAcct + "/" + progFile + "/" + progName
	f, err := client.Create(srcPath)
	if err != nil {
		return fmt.Errorf("failed to create BASIC source file (%s): %s", srcPath, err)
	}

	if _, err := f.Write([]byte(progSrc)); err != nil {
		_ = f.Close()
		return fmt.Errorf("error writing to BASIC source file (%s): %s", srcPath, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("error closing BASIC source file (%s): %s", srcPath, err)
	}

	r, err := c.ExecutePhantom("BASIC " + progFile + " " + progName)
	if err != nil {
		return fmt.Errorf("failed to compile BASIC program: %s", err)
	}
	defer safeClose(r, "failed to close BASIC compile response reader", &err)

	buf, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	// Expecting output of the form: "\nCompiling Unibasic: BP/testProg in mode 'u'.\ncompilation finished\n"
	re := regexp.MustCompile(`\ncompilation finished\n`)
	if matched := re.Match(buf); !matched {
		return fmt.Errorf("unexpected response when compiling BASIC program:\n%s", buf)
	}

	return nil
}

// DeleteBasicProgram deletes the named BASIC program from the UDT server
func (c *Client) DeleteBasicProgram(progFile string, progName string) (err error) {

	if progFile == "" {
		panic("progFile must not be blank")
	}
	if progName == "" {
		panic("progName must not be blank")
	}

	// Initialize SFTP client
	client, err := sftp.NewClient(c.sshClient)
	if err != nil {
		return fmt.Errorf("failed to initialize SFTP client: %s", err)
	}
	defer safeClose(client, "failed to close SFTP client", &err)

	binPath := c.env.UdtAcct + "/" + progFile + "/_" + progName
	if err := client.Remove(binPath); err != nil {
		return fmt.Errorf("failed to delete BASIC program file (%s): %s", binPath, err)
	}

	srcPath := c.env.UdtAcct + "/" + progFile + "/" + progName
	if err := client.Remove(srcPath); err != nil {
		return fmt.Errorf("failed to delete BASIC source file (%s): %s", srcPath, err)
	}

	return nil
}

// SavedListDelete deletes a saved list from the UDT server
func (c *Client) SavedListDelete(savedListName string) (err error) {

	if savedListName == "" {
		panic("savedListName must not be blank")
	}

	r, err := c.ExecutePhantom("DELETELIST '" + savedListName + "'")
	if err != nil {
		return fmt.Errorf("failed to delete UDT saved list (%s): %s", savedListName, err)
	}
	defer safeClose(r, "failed to close PHANTOM response reader", &err)

	buf, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	// Expecting output of the form: "'d5375b81-b09a-6a17-c182-04c272e5f71d' deleted.\n"
	re := regexp.MustCompile(`'` + savedListName + `' deleted\.\n`)
	if matched := re.Match(buf); !matched {
		return fmt.Errorf("unexpected response when deleting saved list:\n%q", buf)
	}

	return nil
}

// RetrieveOutput retrieves the output of the provided PhantomProc
func (c *Client) RetrieveOutput(proc *PhantomProc) (_ io.ReadCloser, err error) {

	// Initialize SFTP client
	client, err := sftp.NewClient(c.sshClient)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize SFTP client: %s", err)
	}

	// Retrieve COMO file and verify the command ran successfully
	f, err := client.Open(proc.OutFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open UDT output file (%s): %s", proc.OutFile, err)
	}

	// Pipe the PHANTOM output through a TruncReader to strip the last line of output
	r := truncatereader.NewTruncReader(f, []byte(fmt.Sprintf("PHANTOM process %d has completed.\n", proc.Pid)))

	return newHookedCloser(r, func() (err error) {
		defer safeClose(client, "failed to close SFTP client", &err)
		if err = f.Close(); err != nil {
			return fmt.Errorf("error closing UDT output file: %s", err)
		}

		// Remove the COMO file
		if err = client.Remove(proc.OutFile); err != nil {
			return fmt.Errorf("error removing temporary COMO file (%s): %s", proc.OutFile, err)
		}
		return nil
	}), nil
}

/*
// RetrieveSavedList retrieves a Unidata saved list created by "SAVE.LIST <listName>"
func (c *Client) RetrieveSavedList(listName string, remove bool) (io.ReadCloser, error) {

	// Initialize SFTP client
	client, err := sftp.NewClient(c.sshClient)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize SFTP client: %s", err)
	}

	savedListFiles, err := client.Glob(c.udtHome + "/SAVEDLISTS/" + listName + "000")

	fHandles := make([]io.Reader, len(savedListFiles))
	for i, savedListFile := range savedListFiles {
		// Retrieve COMO file and verify the command ran successfully
		fHandles[i], err = client.Open(savedListFile)
		if err != nil {
			return nil, fmt.Errorf("failed to open UDT savedlist file (%s): %s", savedListFile, err)
		}
	}

	f := io.MultiReader(fHandles...)

	// Return a hookedCloser that closes the file handles and deletes the savelist files
	if remove {
		return newHookedCloser(f, func() error {
			defer client.Close()

			for _, f := range fHandles {
				f.(io.ReadCloser).Close()
			}

			// Remove the savelist files
			for _, savedListFile := range savedListFiles {
				if err = client.Remove(savedListFile); err != nil {
					return fmt.Errorf("error removing UDT savedlist file (%s): %s", savedListFile, err)
				}
			}
			return nil
		}), nil
	}

	// Return a hookedCloser that closes the file handles
	return newHookedCloser(f, func() error {
		defer client.Close()

		for _, f := range fHandles {
			f.(io.ReadCloser).Close()
		}

		return nil
	}), nil
}
*/

// QuoteString takes a string and returns a UniBasic expression that will evaluate to the provided string.
// Given `He said "It's so warm"`, will return `'He said "It':"'":'s so warm"'`
func QuoteString(str string) string {
	parts := strings.Split(str, `'`)
	return `'` + strings.Join(parts, `':"'":'`) + `'`
}

type hookedCloser struct {
	io.Reader
	closer func() error
}

func (r *hookedCloser) Close() error {
	return r.closer()
}

func newHookedCloser(r io.Reader, closer func() error) io.ReadCloser {
	return &hookedCloser{r, closer}
}

func safeClose(c io.Closer, msg string, err *error) {
	if cerr := c.Close(); cerr != nil && *err == nil {
		*err = fmt.Errorf("%s: %s", msg, cerr)
	}
}

func safeCloseIgnoreEOF(c io.Closer, msg string, err *error) {
	// https://stackoverflow.com/questions/42590308/proper-way-to-close-a-crypto-ssh-session-freeing-all-resources-in-golang#comment72394178_42590388
	// session.Close() will return io.EOF if called after session.Wait()/session.Run()
	// we want to ignore the EOF
	if cerr := c.Close(); cerr != nil && cerr != io.EOF && err == nil {
		*err = fmt.Errorf("%s: %s", msg, cerr)
	}
}
