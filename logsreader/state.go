package logsreader

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
)

const (
	stateFileNamePattern = "state_%d.json"
)

// ErrNoStateFile indicates that state file doesn't exist. Most likely this happens
// because server is being processed first time
var ErrNoStateFile = errors.New("state file doesn't exist")

// State store information about state from previous connection
type State struct {
	// NotZippedLogFile stores the name of only log file that was not zipped yet except access.log.
	// If this name changes it means that nginx has started new log file and archived access.log that we were reading last time
	NotZippedLogFile string

	// BytesRead stores Number of bytes that were already read from access.log
	BytesRead int
}

// GetState returns State object for given server
func GetState(conn ConnectionInfo) (State, error) {
	fileName := buildStateFileName(conn)
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			err = ErrNoStateFile
		} else {
			err = fmt.Errorf("cannot read state from %s: %v", fileName, err)
		}
		return State{}, err
	}

	var stats stateJSON
	err = json.Unmarshal(data, &stats)
	if err != nil {
		return State{}, fmt.Errorf("cannot parse json from %s: %v", fileName, err)
	}

	return State{
		NotZippedLogFile: stats.LastLog,
		BytesRead:        stats.BytesRead,
	}, nil
}

// SaveState saves State for given server
func SaveState(conn ConnectionInfo, stats State) error {
	s := stateJSON{
		LastLog:   stats.NotZippedLogFile,
		BytesRead: stats.BytesRead,
	}

	data, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("cannot serialize state %v: %v", s, err)
	}

	fileName := buildStateFileName(conn)
	err = ioutil.WriteFile(fileName, data, 0777)
	if err != nil {
		return fmt.Errorf("cannot save state to file %s: %v", fileName, err)
	}

	return nil
}

func buildStateFileName(conn ConnectionInfo) string {
	return fmt.Sprintf(stateFileNamePattern, conn.Port)
}

type stateJSON struct {
	LastLog   string `json:"log"`
	BytesRead int    `json:"read"`
}
