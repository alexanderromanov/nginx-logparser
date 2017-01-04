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

const (
	maxBatchSize = 100
)

// SaveConsumptions saves report to azure storage table
func SaveConsumptions(settings AzureStorageSettings, consumptions WebsiteConsumptions, serverName string) error {
	storageClient, err := storage.NewBasicClient(settings.AccountName, settings.Key)
	if err != nil {
		return err
	}

	client := storageClient.GetTableService()
	now := time.Now()
	batches := map[storage.AzureTable]map[int][][]*storage.TableEntity{}
	log.Println(serverName + " - " + "Starting processing of consumptions")
	for websiteID, records := range consumptions {
		for _, stat := range records {
			fields := make(map[string]interface{})
			fields["Time"] = stat.Time.Unix()
			fields["Files"] = stat.Files
			fields["FilesCount"] = stat.FilesCount
			fields["Dynamic"] = stat.Dynamic
			fields["DynamicCount"] = stat.DynamicCount
			fields["Other"] = stat.Other
			fields["OtherCount"] = stat.OtherCount

			entity := &storage.TableEntity{
				PartitionKey: strconv.Itoa(websiteID),
				RowKey:       generateRowKey(stat, serverName, now),
				Fields:       fields,
			}
			usageTable := getOrCreateUsageTable(client, settings, stat.Time)

			tableBatches := batches[usageTable]
			if tableBatches == nil {
				tableBatches = map[int][][]*storage.TableEntity{}
			}
			websiteBatches := tableBatches[websiteID]
			if len(websiteBatches) == 0 {
				websiteBatches = [][]*storage.TableEntity{[]*storage.TableEntity{}}
			}
			latestBatch := websiteBatches[len(websiteBatches)-1]
			if len(latestBatch)+1 >= maxBatchSize {
				latestBatch = []*storage.TableEntity{}
				websiteBatches = append(websiteBatches, latestBatch)
			}
			latestBatch = append(latestBatch, entity)
			websiteBatches[len(websiteBatches)-1] = latestBatch
			tableBatches[websiteID] = websiteBatches
			batches[usageTable] = tableBatches
		}
	}

	log.Println(serverName + " - " + "Initiating saving to Azure")
	var tablesWg sync.WaitGroup
	for table, tableBatches := range batches {
		tablesWg.Add(1)
		go func(table storage.AzureTable, tableBatches map[int][][]*storage.TableEntity) {
			defer tablesWg.Done()
			err := processTableBatches(client, table, tableBatches)
			if err != nil {
				log.Println(err)
			}
		}(table, tableBatches)
	}
	tablesWg.Wait()

	return nil
}

func processTableBatches(client storage.TableServiceClient, table storage.AzureTable, tableBatches map[int][][]*storage.TableEntity) error {
	var websitesWg sync.WaitGroup
	throttle := make(chan bool, 3)
	for _, websiteBatches := range tableBatches {
		throttle <- true
		websitesWg.Add(1)
		go func(websiteBatches [][]*storage.TableEntity) {
			defer websitesWg.Done()
			err := processWebsiteBatches(client, table, websiteBatches)
			if err != nil {
				log.Println(err)
			}
			<-throttle
		}(websiteBatches)
	}
	websitesWg.Wait()
	return nil
}

func processWebsiteBatches(client storage.TableServiceClient, table storage.AzureTable, websiteBatches [][]*storage.TableEntity) error {
	var wg sync.WaitGroup
	throttle := make(chan bool, 6)
	for _, batch := range websiteBatches {
		throttle <- true
		wg.Add(1)
		go func(batch []*storage.TableEntity) {
			defer wg.Done()
			err := client.BatchInsert(table, batch)
			if err != nil {
				log.Println(err)
			}
			<-throttle
		}(batch)
	}
	wg.Wait()
	return nil
}

func generateRowKey(stats *ConsumptionRecord, server string, now time.Time) string {
	return fmt.Sprintf("%d-%s-%d", stats.Time.Unix(), server, now.Unix())
}

var createdTables = make([]storage.AzureTable, 3)

func getOrCreateUsageTable(client storage.TableServiceClient, settings AzureStorageSettings, requestTime time.Time) storage.AzureTable {
	result := storage.AzureTable(settings.TableNameTemplate + requestTime.Format("200601"))
	for _, table := range createdTables {
		if table == result {
			return result
		}
	}

	client.CreateTable(result)
	createdTables = append(createdTables, result)
	return result
}
