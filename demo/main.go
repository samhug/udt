package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"syscall"

	"github.com/samhug/udt"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/terminal"
)

func main() {

	hostPtr := flag.String("host", "", "IP/Hostname of server")
	portPtr := flag.Int("port", 22, "SSH port")
	udtBinPtr := flag.String("udtbin", "/usr/udthome/bin", "$UDTBIN dir")
	udtHomePtr := flag.String("udthome", "/usr/udthome", "$UDTHOME dir")
	udtAcctPtr := flag.String("udtacct", "/usr/udthome/demo", "UDT account dir")

	flag.Parse()

	if *hostPtr == "" {
		flag.Usage()
		return
	}

	username, password := getCredentials()

	sshConfig := &ssh.ClientConfig{
		User: username,
		Auth: []ssh.AuthMethod{
			ssh.Password(password),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	addr := fmt.Sprintf("%s:%d", *hostPtr, *portPtr)

	sshClient, err := ssh.Dial("tcp", addr, sshConfig)
	if err != nil {
		fmt.Printf("SSH unable to connect: %s", err)
		return
	}

	envConfig := &udt.EnvConfig{
		UdtBin:  *udtBinPtr,
		UdtHome: *udtHomePtr,
		UdtAcct: *udtAcctPtr,
	}

	c := udt.NewClient(sshClient, envConfig)

	demoRaw(c, "WHAT")
	//demoRaw(c, "LIST DICT STUDENT")
	//demoRaw(c, "LIST STUDENT LNAME CGA TOXML SAMPLE 1")

	demoQuery(c, "LIST CLIENTS NAME COMPANY ADDRESS SAMPLE 3 TOXML")

	demoQueryBatched(c, &udt.QueryConfig{
		Select:    []string{"SELECT ORDERS WITH ORD_DATE=\"10/25/2000\""},
		File:      "ORDERS",
		Fields:    []string{"ID", "ORD_DATE", "ORD_TIME"},
		BatchSize: 25,
	})

}

func demoRaw(c *udt.Client, statement string) {
	fmt.Println("\n=== Demo Basic ===")
	fmt.Println("=== Execute a raw statement and retrieve the output")

	proc, err := c.ExecutePhantomAsync(statement)
	if err != nil {
		fmt.Printf("UDT Execute failed: %s", err)
		return
	}

	if err := c.WaitPhantom(proc); err != nil {
		fmt.Printf("error waiting for process to terminate: %s", err)
		return
	}

	buf, err := c.RetrieveOutput(proc)
	if err != nil {
		fmt.Printf("unable to retrieve UDT output: %s", err)
		return
	}

	out, err := ioutil.ReadAll(buf)
	if err != nil {
		fmt.Printf("unable to read UDT output: %s", err)
		return
	}
	fmt.Println(string(out))
}

func demoQuery(c *udt.Client, query string) {
	fmt.Println("\n=== Demo Query ===")
	fmt.Printf("=== Run the query: %s\n", query)

	q := udt.NewQuery(query)
	r, err := q.Run(c)
	if err != nil {
		fmt.Printf("Error running query: %s", err)
		return
	}
	defer r.Close()

	for {
		record, err := r.ReadRecord()
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("Error: %s\n", err)
			break
		}

		fmt.Printf("%q\n", record)
	}
}

func demoQueryBatched(c *udt.Client, queryCfg *udt.QueryConfig) {
	fmt.Println("\n=== Demo Query Batched ===")
	fmt.Printf("=== Run the query:\n=== %#v\n", queryCfg)

	r, err := udt.NewQueryBatched(c, queryCfg)
	if err != nil {
		fmt.Printf("Error running query: %s", err)
		return
	}
	defer r.Close()

	for {
		record, err := r.ReadRecord()
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("Error: %s\n", err)
			break
		}

		fmt.Printf("%q\n", record)
	}
}

// From https://stackoverflow.com/a/32768479/2069095
func getCredentials() (string, string) {
	reader := bufio.NewReader(os.Stdin)

	fmt.Print("Enter Username: ")
	username, _ := reader.ReadString('\n')

	fmt.Print("Enter Password: ")
	bytePassword, _ := terminal.ReadPassword(int(syscall.Stdin))
	password := string(bytePassword)
	fmt.Println()

	return strings.TrimSpace(username), strings.TrimSpace(password)
}
