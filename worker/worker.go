// Worker that processes mailrail jobs from a pqueue.
package main

import (
	"flag"
	"fmt"
	"github.com/ljosa/mailrail"
	"os"
	"path"
)

func main() {
	var doNotSend bool
	var simulator bool
	var sendTo string

	flag.Usage = usage
	flag.BoolVar(&doNotSend, "donotsend", false,
		"do not send any emails")
	flag.BoolVar(&simulator, "simulator", false,
		"send emails to AWS simulator")
	flag.StringVar(&sendTo, "sendto", "",
		"send all emails to this address")
	flag.Parse()
	if len(flag.Args()) != 1 {
		flag.Usage()
		os.Exit(1)
	}
	queueDir := flag.Args()[0]

	var mangler mailrail.Mangler
	switch {
	case doNotSend:
		mangler = mailrail.DoNotSend
	case simulator:
		mangler = mailrail.SendToSimulator
	case sendTo != "":
		mangler = mailrail.SendToMe(sendTo)
	default:
		mangler = mailrail.DoNotMangle
	}
	mailrail.ProcessForever(queueDir, mangler)
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s QUEUE-DIR\n", path.Base(os.Args[0]))
	flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, "\nYou must set the AWS_DEFAULT_REGION environment variable\n(e.g., to `us-east-1`).\n")
}
