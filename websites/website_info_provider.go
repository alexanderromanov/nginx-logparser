package websites

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// DomainsInfoProviderSettings contains settings required to connect to DomainInfo provider
type DomainsInfoProviderSettings struct {
	URL                 string
	UserName            string
	Password            string
	ServiceDomainSuffix string
}

// WebsiteInfo provides basic information about website
type WebsiteInfo struct {
	ID int
}

// GetDomains returns map of type DomainName -> WebsiteInfo
func GetDomains(settings DomainsInfoProviderSettings) (map[string]*WebsiteInfo, error) {
	if err := settings.validate(); err != nil {
		return nil, err
	}

	form := url.Values{}
	form.Add("username", settings.UserName)
	form.Add("password", settings.Password)
	req, err := http.NewRequest("POST", settings.URL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("HTTP Response Error %d\n", resp.StatusCode)
	}

	var domains []websiteInfoJSON
	err = json.NewDecoder(resp.Body).Decode(&domains)

	if err != nil {
		return nil, err
	}

	result := map[string]*WebsiteInfo{}
	for _, line := range domains {
		key, value := processWebsiteInfoJSON(&line)

		result[key] = value

		if !strings.HasSuffix(key, settings.ServiceDomainSuffix) {
			result["www."+key] = value
		}
	}

	return result, nil
}

type domainsList struct {
	Domains []websiteInfoJSON `json:"domains"`
}

type websiteInfoJSON struct {
	Domain string `json:"d"`
	ID     int    `json:"w"`
}

func processWebsiteInfoJSON(websiteInfo *websiteInfoJSON) (string, *WebsiteInfo) {
	key := strings.ToLower(websiteInfo.Domain)
	value := WebsiteInfo{ID: websiteInfo.ID}

	return key, &value
}

func (settings *DomainsInfoProviderSettings) validate() error {
	if settings.URL == "" {
		return errors.New("URL was not provided")
	}

	if settings.UserName == "" {
		return errors.New("Username was not provided")
	}

	if settings.Password == "" {
		return errors.New("Password was not provided")
	}

	if settings.ServiceDomainSuffix == "" {
		return errors.New("Service Domain Suffix was not provided")
	}

	return nil
}
