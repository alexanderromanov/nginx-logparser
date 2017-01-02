package logsreader

import (
	"errors"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// LogRecord represents a line of nginx log file
type LogRecord struct {
	IPAddress      string
	Time           time.Time
	Duration       float64
	Verb           string
	Path           string
	HTTPStatusCode int
	Size           int
	Domain         string
	Referrer       string
	UserAgent      string
}

// ParseLine parses line of nginx logs
// Expected line looks like this: "111.111.111.111(-)" "[31/Jul/2016:22:54:30 +0400]" "0.247" "GET /some/file.jpg HTTP/1.1" "200" "32327" "some-domain.com" "http://some-referrer.com/" "User Agent String"
func parseLine(line string) (*LogRecord, error) {
	results, err := splitLine(line)
	if err != nil {
		return nil, err
	}
	if len(results) != 9 {
		return nil, errors.New("Please double check nginx log line format. It should contain Ip Address, Date, Request Duration, Path, Response Status, Response Size, Domain, Referrer, User Agent in this particular order")
	}

	date, err := time.Parse("[02/Jan/2006:15:04:05 -0700]", results[1])
	if err != nil {
		return nil, err
	}

	duration, err := strconv.ParseFloat(results[2], 64)
	if err != nil {
		return nil, err
	}

	requestStrings := strings.Split(results[3], " ")
	if len(requestStrings) < 3 {
		return nil, errors.New("Fail to parse request string: " + results[3])
	}
	verb := requestStrings[0]
	path := strings.Join(requestStrings[1:len(requestStrings)-1], " ")

	httpStatusCode, err := strconv.Atoi(results[4])
	if err != nil {
		return nil, err
	}

	size, err := strconv.Atoi(results[5])
	if err != nil {
		return nil, err
	}

	return &LogRecord{
		Domain:         results[6],
		Duration:       duration,
		Path:           path,
		Verb:           verb,
		IPAddress:      results[0][:strings.Index(results[0], "(")],
		HTTPStatusCode: httpStatusCode,
		Time:           date.UTC(),
		Referrer:       results[7],
		UserAgent:      results[8],
		Size:           size,
	}, nil
}

var lineSplitRegex = regexp.MustCompile(`\"(.*?)\"`)

func splitLine(line string) ([]string, error) {
	matches := lineSplitRegex.FindAllStringSubmatch(line, -1)
	if len(matches) == 0 {
		return nil, errors.New("Cannot split line: " + line)
	}

	result := make([]string, len(matches))
	for i, str := range matches {
		result[i] = str[1]
	}

	return result, nil
}
