package logsreader

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// ReadLogs read logs from server
func ReadLogs(connection ConnectionInfo, readerState State, recordProcessor func(*LogRecord)) (*State, error) {
	sftp, err := connectToServer(connection)
	if err != nil {
		return nil, fmt.Errorf("fail to connect to server %s: %v", conn, err)
	}
	defer sftp.Close()

	notZipped := findNotZippedLog(sftp)

	var logOffset int
	if readerState.NotZippedLogFile == notZipped {
		logOffset = readerState.BytesRead
	} else {
		logOffset = 0

		_, err = processRecords(sftp, notZipped, readerState.BytesRead, recordProcessor)
		if err != nil {
			return nil, err
		}
	}

	bytesRead, err := processRecords(sftp, "/var/log/nginx/access.log", logOffset, recordProcessor)
	if err != nil {
		return nil, err
	}

	newState := &State{
		NotZippedLogFile: notZipped,
		BytesRead:        bytesRead + logOffset,
	}

	return newState, nil
}

func connectToServer(connection ConnectionInfo) (*sftp.Client, error) {
	clientConfig := &ssh.ClientConfig{
		User: connection.UserName,
		Auth: []ssh.AuthMethod{
			ssh.Password(connection.Password),
		},
	}

	addressWithPort := fmt.Sprintf("%s:%d", connection.Address, connection.Port)
	client, err := ssh.Dial("tcp", addressWithPort, clientConfig)

	if err != nil {
		return nil, fmt.Errorf("cannot dial remote server: %v", err)
	}

	sftp, err := sftp.NewClient(client)
	if err != nil {
		return nil, fmt.Errorf("fail to create sftp client: %v", err)
	}

	return sftp, nil
}

func findNotZippedLog(sftp *sftp.Client) string {
	w := sftp.Walk("/var/log/nginx/")
	for w.Step() {
		if err := w.Err(); err != nil {
			continue
		}

		fullPath := w.Path()
		fileName := path.Base(fullPath)

		if strings.HasPrefix(fileName, "access.log-") && !strings.HasSuffix(fileName, ".gz") {
			return fullPath
		}
	}

	return ""
}

func processRecords(client *sftp.Client, fileName string, readFrom int, recordProcessor func(*LogRecord)) (int, error) {
	log.Printf("opening file %s\n", fileName)
	file, err := client.Open(fileName)
	if err != nil {
		return 0, fmt.Errorf("cannot open %s: %v", fileName, err)
	}

	defer file.Close()

	_, err = file.Seek(int64(readFrom), os.SEEK_SET)
	if err != nil {
		return 0, fmt.Errorf("cannot seek to %d in %s: %v", readFrom, fileName, err)
	}

	log.Printf("reading file %s from position %d\n", fileName, readFrom)

	bytesRead := 0
	scanner := bufio.NewScanner(file)

	var throttle = make(chan bool, 200)
	var wg sync.WaitGroup
	for scanner.Scan() {
		logLine := scanner.Text()

		throttle <- true
		wg.Add(1)
		go func(logLine string) {
			defer wg.Done()
			logRecord, err := parseLine(logLine)
			if err != nil {
				log.Printf("fail to parse %s\n", logLine)
				return
			}

			recordProcessor(logRecord)
			<-throttle
		}(logLine)

		// 1 is length of line separator (\n)
		bytesRead += len(logLine) + 1
	}
	wg.Wait()

	return bytesRead, nil
}
