package logsreader

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"log"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// LogReaderResult stores information about log reading attempt
type LogReaderResult struct {
	UnparsedLines []string
	Records       []*LogRecord
	ReaderState   *State
}

// ReadLogs read logs from server
func ReadLogs(connection ConnectionInfo, readerState State) (LogReaderResult, error) {
	clientConfig := &ssh.ClientConfig{
		User: connection.UserName,
		Auth: []ssh.AuthMethod{
			ssh.Password(connection.Password),
		},
	}

	addressWithPort := fmt.Sprintf("%s:%d", connection.Address, connection.Port)
	client, err := ssh.Dial("tcp", addressWithPort, clientConfig)

	if err != nil {
		return LogReaderResult{}, err
	}

	sftp, err := sftp.NewClient(client)
	if err != nil {
		return LogReaderResult{}, err
	}
	defer sftp.Close()

	notZipped := findNotZippedLog(sftp)

	return readAccessLog(sftp, readerState, notZipped)
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

// readAccessLog reads currently active log file (access.log) and log file from previous day starting from saved position
func readAccessLog(client *sftp.Client, previousStats State, currentNotZippedName string) (LogReaderResult, error) {
	var records []*LogRecord
	var failedLines []string
	logOffset := previousStats.ReadFromAccessLog
	if previousStats.NotZippedLogFile != currentNotZippedName {
		var err error
		records, failedLines, _, err = getRecords(client, currentNotZippedName, logOffset)
		if err != nil {
			return LogReaderResult{}, err
		}

		// access.log is a fresh file, so we need to reset offset
		logOffset = 0
	}

	accessRecords, accessFailedLines, nBytes, err := getRecords(client, "/var/log/nginx/access.log", logOffset)
	if err != nil {
		return LogReaderResult{}, err
	}

	records = append(records, accessRecords...)
	failedLines = append(failedLines, accessFailedLines...)

	result := LogReaderResult{
		UnparsedLines: failedLines,
		Records:       records,
		ReaderState: &State{
			NotZippedLogFile:  currentNotZippedName,
			ReadFromAccessLog: nBytes + logOffset},
	}

	return result, nil
}

func getRecords(client *sftp.Client, fileName string, readFrom int64) ([]*LogRecord, []string, int64, error) {
	log.Printf("Opening file %s\n", fileName)
	file, err := client.Open(fileName)
	if err != nil {
		return nil, nil, 0, err
	}

	defer file.Close()

	log.Printf("Reading file %s from position %d\n", fileName, readFrom)
	contentRead, bytesRead, err := readFileToTheEnd(file, readFrom)
	if err != nil {
		return nil, nil, 0, err
	}

	log.Printf("%d bytes read from file %s", bytesRead, fileName)

	var result []*LogRecord
	var failedLines []string
	scanner := bufio.NewScanner(bytes.NewReader(contentRead))
	for scanner.Scan() {
		logLine := scanner.Text()
		parsedLine, err := parseLine(logLine)

		if err != nil {
			failedLines = append(failedLines, logLine)
			continue
		}

		result = append(result, parsedLine)
	}

	return result, failedLines, bytesRead, nil
}

func readFileToTheEnd(file *sftp.File, readFrom int64) ([]byte, int64, error) {
	_, err := file.Seek(readFrom, os.SEEK_SET)
	if err != nil {
		return nil, 0, err
	}

	bytesRead := int64(0)
	r := bufio.NewReader(file)
	buf := make([]byte, 0, 40*1024)
	contentRead := make([]byte, 10*1024)
	for {
		n, err := r.Read(buf[:cap(buf)])
		buf = buf[:n]
		if n == 0 {
			if err == nil {
				continue
			}
			if err == io.EOF {
				break
			}
			return nil, bytesRead, err
		}
		bytesRead += int64(len(buf))

		contentRead = append(contentRead, buf...)

		if err != nil && err != io.EOF {
			return nil, bytesRead, err
		}
	}

	return contentRead, bytesRead, nil
}
