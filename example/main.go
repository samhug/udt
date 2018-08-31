package main

import (
	"bufio"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"syscall"

	"github.com/samuelhug/udt"
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

	conn := udt.NewConnection(sshClient, *udtBinPtr, *udtHomePtr, *udtAcctPtr)

	fmt.Println("=== Example Basic ===")
	fmt.Println("Query basic server information")

	// WHAT
	// LIST DICT STUDENT
	// LIST STUDENT LNAME CGA TOXML SAMPLE 1
	proc, err := conn.ExecutePhantomAsync("LIST CUSTOMER NAME TAPE_INFO TOXML SAMPLE 1")
	if err != nil {
		fmt.Printf("UDT Execute failed: %s", err)
		return
	}

	if err := conn.Wait(proc); err != nil {
		fmt.Printf("error waiting for process to terminate: %s", err)
		return
	}

	buf, err := conn.RetrieveOutput(proc)
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

	/*
		q := "LIST CLIENTS NAME COMPANY ADDRESS SAMPLE 3 TOXML"

		fmt.Println("=== Example Query ===")
		fmt.Println("Run the query:", q)

		query := udt.NewQuery(q)
		r, err := query.Run(conn)
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
	*/
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
