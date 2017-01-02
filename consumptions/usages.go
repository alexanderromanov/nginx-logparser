package consumptions

import (
	"strconv"
	"strings"
	"time"

	"sync"

	"github.com/alexanderromanov/nginx-logparser/logsreader"
	"github.com/alexanderromanov/nginx-logparser/websites"
)

// UsagesCollection contains methods to calculate traffic stats from log records
type UsagesCollection struct {
	usagesSync     sync.RWMutex
	domainsSync    sync.RWMutex
	unknownSync    sync.RWMutex
	usages         map[string]*ConsumptionRecord
	domains        map[string]*websites.WebsiteInfo
	unknownDomains map[string]int
}

// NewUsagesCollection creates instance of UsagesCollection
func NewUsagesCollection(domains map[string]*websites.WebsiteInfo) *UsagesCollection {
	usages := map[string]*ConsumptionRecord{}
	unknownDomains := map[string]int{}
	return &UsagesCollection{
		usages:         usages,
		domains:        domains,
		unknownDomains: unknownDomains,
	}
}

// ConsumptionRecord contains information about traffic consumption of a website
type ConsumptionRecord struct {
	WebsiteID    int
	Time         time.Time
	FilesCount   int
	Files        int64
	Dynamic      int64
	DynamicCount int
	Other        int64
	OtherCount   int
}

// UnknownDomainsCounter contains information about domains unknown to the system and number
// of times they were requested
type UnknownDomainsCounter struct {
	Domain    string
	Requested int
}

// AddRecord adds log record to UsagesCollection
func (usages *UsagesCollection) AddRecord(record *logsreader.LogRecord) {
	if shouldIgnore(record) {
		return
	}

	usages.domainsSync.RLock()
	website, ok := usages.domains[record.Domain]
	usages.domainsSync.RUnlock()
	if !ok {
		usages.addUnknownDomain(record.Domain)
		return
	}

	hour := getHour(record.Time)
	usageKey := strconv.Itoa(website.ID) + "-" + strconv.FormatInt(hour.Unix(), 10)
	usages.usagesSync.RLock()
	usageRecord, ok := usages.usages[usageKey]
	usages.usagesSync.RUnlock()
	if !ok {
		usageRecord = &ConsumptionRecord{WebsiteID: website.ID, Time: hour}
		usages.usagesSync.Lock()
		usages.usages[usageKey] = usageRecord
		usages.usagesSync.Unlock()
	}

	switch {
	case isFile(record.Path):
		usageRecord.Files += int64(record.Size)
		usageRecord.FilesCount++
	case isOther(record.HTTPStatusCode):
		usageRecord.Other += int64(record.Size)
		usageRecord.OtherCount++
	default:
		usageRecord.Dynamic += int64(record.Size)
		usageRecord.DynamicCount++
	}
}

// GetTrafficConsumption returns traffic consumptions of currently added log records
func (usages *UsagesCollection) GetTrafficConsumption() []*ConsumptionRecord {
	result := make([]*ConsumptionRecord, len(usages.usages))
	i := 0
	for _, value := range usages.usages {
		result[i] = value
		i++
	}

	return result
}

// GetUnknownDomains return list of unknown domains found in log records
func (usages *UsagesCollection) GetUnknownDomains() []UnknownDomainsCounter {
	result := make([]UnknownDomainsCounter, len(usages.unknownDomains))
	i := 0
	for domain, count := range usages.unknownDomains {
		result[i] = UnknownDomainsCounter{Domain: domain, Requested: count}
		i++
	}

	return result
}

func (usages *UsagesCollection) addUnknownDomain(domain string) {
	usages.unknownSync.Lock()
	usages.unknownDomains[domain] = usages.unknownDomains[domain] + 1
	usages.unknownSync.Unlock()
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
