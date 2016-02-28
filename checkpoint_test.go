package mailrail

import (
	"github.com/ljosa/go-pqueue/pqueue"
	"io/ioutil"
	"os"
	"testing"
)

func TestCheckpoint(t *testing.T) {
	dir, err := ioutil.TempDir("/tmp", "test_checkpoint_")
	if err != nil {
		t.Fatal("failed to create temp dir for queue", err)
	}
	defer os.RemoveAll(dir)
	q, err := pqueue.OpenQueue(dir)
	j, err := q.CreateJob("foo")
	if err != nil {
		t.Fatal("failed to create job:", err)
	}
	i, err := getCheckpoint(j)
	if err != nil {
		t.Fatal("got unexpected error when trying to get missing checkpoint")
	}
	if i != 0 {
		t.Fatal("got %d instead of 0 when getting missing checkpoint", i)
	}
	err = setCheckpoint(j, 42)
	if err != nil {
		t.Fatal("failed to set checkpoint:", err)
	}
	i, err = getCheckpoint(j)
	if err != nil {
		t.Fatal("failed to get checkpoint:", err)
	}
	if i != 42 {
		t.Fatal("getting checkpoint returned unexpected", i)
	}
}
