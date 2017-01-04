package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"sync"

	"github.com/alexanderromanov/nginx-logparser/consumptions"
	"github.com/alexanderromanov/nginx-logparser/logsreader"
	"github.com/alexanderromanov/nginx-logparser/websites"
)

const (
	settingsFile = "settings.json"
)

func main() {
	log.Println("Initializing application. Reading settings")
	settings, err := getSettings(settingsFile)
	if err != nil {
		log.Println(err)
		return
	}

	log.Println("Getting domains list")
	domains, err := websites.GetDomains(settings.WebsitesProvider)
	if err != nil {
		log.Println(err)
		return
	}
	log.Printf("%d domain records obtained\n", len(domains))

	var wg sync.WaitGroup
	wg.Add(len(settings.Servers))
	for _, conn := range settings.Servers {
		go func(connection logsreader.ConnectionInfo) {
			defer wg.Done()
			err := processLogs(settings, connection, domains)
			if err != nil {
				log.Println(err)
			}
			log.Printf("%s logs are processed", connection.ServerName())
		}(conn)
	}
	wg.Wait()
}

func processLogs(settings applicationSettings, conn logsreader.ConnectionInfo, domains map[string]*websites.WebsiteInfo) error {
	serverName := conn.ServerName()
	logForServer := func(format string, v ...interface{}) {
		log.Printf(serverName+" - "+format+"\n", v...)
	}

	logForServer("Getting connection state")
	prevState, err := logsreader.GetState(conn)
	if err != nil {
		return err
	}

	usages := consumptions.NewUsagesCollection(domains)

	newState, err := logsreader.ReadLogs(conn, prevState, usages.AddRecord)
	if err != nil {
		return err
	}

	for _, domain := range usages.GetUnknownDomains() {
		logForServer("Cannot find info for %s requested %d times", domain.Domain, domain.Requested)
	}

	logForServer("Saving connection state")
	err = logsreader.SaveState(conn, *newState)

	consumptionRecords := usages.GetTrafficConsumption()
	logForServer("Saving consumption records for %d websites", len(consumptionRecords))
	err = consumptions.SaveConsumptions(settings.AzureStorage, consumptionRecords, serverName)
	return err
}

// getSettings returns application settings stored in settingsFile
func getSettings(settingsFile string) (applicationSettings, error) {
	data, err := ioutil.ReadFile(settingsFile)
	if err != nil {
		return applicationSettings{}, err
	}

	var settings settingsJSON
	err = json.Unmarshal(data, &settings)

	if err != nil {
		return applicationSettings{}, err
	}

	servers := make([]logsreader.ConnectionInfo, len(settings.Servers))
	for i, c := range settings.Servers {
		servers[i] = logsreader.ConnectionInfo{
			Address:  c.Address,
			Port:     c.Port,
			UserName: c.UserName,
			Password: c.Password,
		}
	}

	return applicationSettings{
		WebsitesProvider: websites.DomainsInfoProviderSettings{
			URL:                 settings.WebsitesProvider.URL,
			UserName:            settings.WebsitesProvider.UserName,
			Password:            settings.WebsitesProvider.Password,
			ServiceDomainSuffix: settings.WebsitesProvider.ServiceDomainSuffix,
		},
		Servers: servers,
		AzureStorage: consumptions.AzureStorageSettings{
			AccountName:       settings.Azure.AccountName,
			Key:               settings.Azure.Key,
			TableNameTemplate: settings.Azure.TableTemplate,
		},
	}, nil
}

type applicationSettings struct {
	AzureStorage     consumptions.AzureStorageSettings
	Servers          []logsreader.ConnectionInfo
	WebsitesProvider websites.DomainsInfoProviderSettings
}

type settingsJSON struct {
	Azure            azureJSON            `json:"azure"`
	Servers          []connectionInfoJSON `json:"servers"`
	WebsitesProvider websitesProviderJSON `json:"websitesProvider"`
}

type azureJSON struct {
	AccountName   string `json:"accountName"`
	Key           string `json:"key"`
	TableTemplate string `json:"tableTemplate"`
}

type websitesProviderJSON struct {
	URL                 string `json:"url"`
	UserName            string `json:"username"`
	Password            string `json:"password"`
	ServiceDomainSuffix string `json:"serviceDomainSuffix"`
}

type connectionInfoJSON struct {
	Address  string `json:"address"`
	Port     int    `json:"port"`
	UserName string `json:"userName"`
	Password string `json:"password"`
}
