package consumptions

import (
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/alexanderromanov/nginx-logparser/logsreader"
	"github.com/alexanderromanov/nginx-logparser/websites"
)

// UsageStats contains information about traffic consumption of a website
type UsageStats struct {
	WebsiteID int
	Time      time.Time
	Files     int64
	Dynamic   int64
	Other     int64
}

// GetTrafficConsumption calculates traffic consumption of websites based on nginx log records
func GetTrafficConsumption(logRecords []*logsreader.LogRecord, domains map[string]*websites.WebsiteInfo) []*UsageStats {
	usage := map[string]*UsageStats{}

	for _, record := range logRecords {
		if shouldIgnore(record) {
			continue
		}

		website, ok := domains[record.Domain]
		if !ok {
			log.Printf("Cannot find information for domain %s\n", record.Domain)
			continue
		}

		hour := getHour(record.Time)
		usageKey := strconv.Itoa(website.ID) + "-" + strconv.FormatInt(hour.Unix(), 10)
		usageRecord, ok := usage[usageKey]
		if !ok {
			usageRecord = &UsageStats{WebsiteID: website.ID, Files: 0, Dynamic: 0, Other: 0, Time: hour}
			usage[usageKey] = usageRecord
		}

		switch {
		case isFile(record.Path):
			usageRecord.Files += int64(record.Size)
		case isOther(record.HTTPStatusCode):
			usageRecord.Other += int64(record.Size)
		default:
			usageRecord.Dynamic += int64(record.Size)
		}
	}

	result := make([]*UsageStats, len(usage))
	i := 0
	for _, value := range usage {
		result[i] = value
		i++
	}

	return result
}

func getHour(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, time.UTC)
}

func isFile(path string) bool {
	return strings.HasPrefix(path, "/filestore/")
}

func isOther(statusCode int) bool {
	return statusCode == 400
}

var domainsToIgnore = map[string]bool{
	"cdn.redham.ru": true,
	"*":             true,
}

func shouldIgnore(record *logsreader.LogRecord) bool {
	if record.HTTPStatusCode == 410 {
		return true
	}

	_, found := domainsToIgnore[record.Domain]

	return found
}
