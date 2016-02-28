// The standalone command adds a spec to a pqueue then processes a
// single job from that queue.
package main

import (
	"flag"
	"fmt"
	"github.com/ljosa/go-pqueue/pqueue"
	"github.com/ljosa/mailrail"
	"io/ioutil"
	"log"
	"os"
	"path"
)

func main() {
	flag.Usage = usage
	flag.Parse()
	if len(flag.Args()) != 2 {
		flag.Usage()
		os.Exit(1)
	}
	queueDir := flag.Args()[0]
	specFilename := flag.Args()[1]
	spec, err := ioutil.ReadFile(specFilename)
	if err != nil {
		log.Fatalf("Failed to open spec file %s: %s", specFilename, err)
	}
	q, err := pqueue.OpenQueue(queueDir)
	if err != nil {
		log.Fatalf("Failed to open queue %s: %s", queueDir, err)
	}
	j, err := q.CreateJob("standalone")
	j.Set("spec", spec)
	j.Submit()
	mailrail.ProcessOne(queueDir, mailrail.DoNotMangle)
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s QUEUE-DIR SPEC-FILE\n", path.Base(os.Args[0]))
	flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, "\nYou must set the AWS_DEFAULT_REGION environment variable\n(e.g., to `us-east-1`).\n")
}
