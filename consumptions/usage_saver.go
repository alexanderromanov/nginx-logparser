package consumptions

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/alexanderromanov/nginx-logparser/azure-storage"
)

// AzureStorageSettings contains information necessary to save consumption information into Azure Storage tables
type AzureStorageSettings struct {
	AccountName       string
	Key               string
	TableNameTemplate string
}

// SaveUsageRecords saves report to azure storage table
func SaveUsageRecords(settings AzureStorageSettings, usageStats []*ConsumptionRecord, serverName string) error {
	storageClient, err := storage.NewBasicClient(settings.AccountName, settings.Key)
	if err != nil {
		return err
	}

	client := storageClient.GetTableService()

	var wg sync.WaitGroup
	// maximum of 10 requests can be sent the same time
	// todo aromanov: consider making batch request(s) instead
	var throttle = make(chan bool, 10)
	now := time.Now()
	for _, stat := range usageStats {
		fields := make(map[string]interface{})
		fields["Time"] = stat.Time.Unix()
		fields["Files"] = stat.Files
		fields["FilesCount"] = stat.FilesCount
		fields["Dynamic"] = stat.Dynamic
		fields["DynamicCount"] = stat.DynamicCount
		fields["Other"] = stat.Other
		fields["OtherCount"] = stat.OtherCount

		usageTable := getOrCreateUsageTable(client, settings, stat.Time)

		entity := storage.TableEntity{
			PartitionKey: strconv.Itoa(stat.WebsiteID),
			RowKey:       generateRowKey(stat, serverName, now),
			Fields:       fields,
		}
		throttle <- true
		wg.Add(1)
		go func(entity storage.TableEntity) {
			defer wg.Done()
			err := client.InsertEntity(usageTable, entity)
			if err != nil {
				log.Println(err)
			}
			<-throttle
		}(entity)
	}
	wg.Wait()

	return nil
}

func generateRowKey(stats *ConsumptionRecord, server string, now time.Time) string {
	return fmt.Sprintf("%d-%s-%d", stats.Time.Unix(), server, now.Unix())
}

var tables = make([]storage.AzureTable, 3)

func getOrCreateUsageTable(
	client storage.TableServiceClient,
	settings AzureStorageSettings,
	requestTime time.Time) storage.AzureTable {
	result := storage.AzureTable(settings.TableNameTemplate + requestTime.Format("200601"))
	for _, table := range tables {
		if table == result {
			return result
		}
	}

	_ = client.CreateTable(result)
	return result
}
