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
	AccountName string
	Key         string
	TableName   string
}

// SaveUsageRecords saves report to azure storage table
func SaveUsageRecords(settings AzureStorageSettings, usageStats []*UsageStats, serverName string) error {
	storageClient, err := storage.NewBasicClient(settings.AccountName, settings.Key)
	if err != nil {
		return err
	}

	usageTable := storage.AzureTable(settings.TableName)
	client := storageClient.GetTableService()

	var wg sync.WaitGroup
	// maximum of 15 requests can be sent the same time
	var throttle = make(chan bool, 15)
	now := time.Now()
	for _, stat := range usageStats {
		fields := make(map[string]interface{})
		fields["Time"] = stat.Time.Unix()
		fields["Files"] = stat.Files
		fields["Dynamic"] = stat.Dynamic
		fields["Other"] = stat.Other

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

func generateRowKey(stats *UsageStats, server string, now time.Time) string {
	return fmt.Sprintf("%d-%s-%d", stats.Time.Unix(), server, now.Unix())
}
