package logsreader

import (
	"encoding/json"
	"fmt"
	"os"
	"io/ioutil"
)

const (
	stateFileNamePattern = "state_%d.json"
)

// State store information about state from previous connection
type State struct {
	// NotZippedLogFile stores the name of only log file that was not zipped yet except access.log.
	// If this name changes it means that nginx has started new log file and archived access.log that we were reading last time
	NotZippedLogFile string

	// ReadFromAccessLog stores Number of bytes that were already read from access.log
	ReadFromAccessLog int64
}

// GetState returns State object for given server
func GetState(conn ConnectionInfo) (State, error) {
	data, err := ioutil.ReadFile(buildStateFileName(conn))
	if err != nil {
		if os.IsNotExist(err){
			err = nil
		}
		return State{}, err
	}

	var stats stateJSON
	err = json.Unmarshal(data, &stats)

	if err != nil {
		return State{}, err
	}

	return State{
		NotZippedLogFile:  stats.LastLog,
		ReadFromAccessLog: stats.BytesRead,
	}, nil
}

// SaveState saves State for given server
func SaveState(conn ConnectionInfo, stats State) error {
	s := stateJSON{
		LastLog:   stats.NotZippedLogFile,
		BytesRead: stats.ReadFromAccessLog,
	}

	data, err := json.Marshal(s)

	if err != nil {
		return err
	}

	ioutil.WriteFile(buildStateFileName(conn), data, 0777)
	return nil
}

func buildStateFileName(conn ConnectionInfo) string {
	return fmt.Sprintf(stateFileNamePattern, conn.Port)
}

type stateJSON struct {
	LastLog   string `json:"log"`
	BytesRead int64  `json:"read"`
}
