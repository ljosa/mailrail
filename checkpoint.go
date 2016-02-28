package mailrail

import (
	"encoding/json"
	"fmt"
	"github.com/ljosa/go-pqueue/pqueue"
	"os"
)

type checkpoint struct {
	RecipientsSent int `json:"recipients_sent"`
}

const name string = "recipients_sent"

func setCheckpoint(job *pqueue.Job, i int) error {
	checkpointBytes, err := json.Marshal(checkpoint{i})
	if err != nil {
		return fmt.Errorf("Job %s failed to marshal checkpoint after %d recipients: %s", job.Basename, i, err)
	}
	if err := job.Set(name, checkpointBytes); err != nil {
		return fmt.Errorf("Job %s failed to checkpoint after %d recipients: %s", job.Basename, i, err)
	}
	return nil
}

func getCheckpoint(job *pqueue.Job) (int, error) {
	checkpointBytes, err := job.Get(name)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	var checkpoint checkpoint
	if err := json.Unmarshal(checkpointBytes, &checkpoint); err != nil {
		return 0, fmt.Errorf("Cannot parse contents of %s: %s", name, err)
	}
	return checkpoint.RecipientsSent, nil
}
