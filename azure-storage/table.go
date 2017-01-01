package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

// TableServiceClient contains operations for Microsoft Azure Table Storage
// Service.
type TableServiceClient struct {
	client Client
}

// AzureTable is the typedef of the Azure Table name
type AzureTable string

const (
	tablesURIPath            = "/Tables"
	statusTableAlreadyExists = "TableAlreadyExists"
	statusCreated            = "Created"
)

type createTableRequest struct {
	TableName string `json:"TableName"`
}

func pathForTable(table AzureTable) string { return fmt.Sprintf("%s", table) }

func (c *TableServiceClient) getStandardHeaders() map[string]string {
	return map[string]string{
		"x-ms-version":   "2015-02-21",
		"x-ms-date":      currentTimeRfc1123Formatted(),
		"Accept":         "application/json;odata=nometadata",
		"Accept-Charset": "UTF-8",
		"Content-Type":   "application/json",
	}
}

// CreateTable creates the table given the specific
// name. This function fails if the name is not compliant
// with the specification.
func (c *TableServiceClient) CreateTable(table AzureTable) error {
	uri := c.client.getEndpoint(tableServiceName, tablesURIPath, url.Values{})

	headers := c.getStandardHeaders()

	req := createTableRequest{TableName: string(table)}
	buf := new(bytes.Buffer)
	if err := json.NewEncoder(buf).Encode(req); err != nil {
		return err
	}

	headers["Content-Length"] = fmt.Sprintf("%d", buf.Len())

	resp, err := c.client.execTable("POST", uri, headers, buf)
	if err != nil {
		return err
	}
	defer resp.body.Close()

	if err := checkRespStatus(resp.status, []string{statusCreated, statusTableAlreadyExists}); err != nil {
		return err
	}

	return nil
}

// DeleteTable deletes the table given the specific
// name. This function fails if the table is not present.
// Be advised: DeleteTable deletes all the entries
// that may be present.
func (c *TableServiceClient) DeleteTable(table AzureTable) error {
	uri := c.client.getEndpoint(tableServiceName, tablesURIPath, url.Values{})
	uri += fmt.Sprintf("('%s')", string(table))

	headers := c.getStandardHeaders()

	headers["Content-Length"] = "0"

	resp, err := c.client.execTable("DELETE", uri, headers, nil)

	if err != nil {
		return err
	}
	defer resp.body.Close()

	if err := checkRespCode(resp.statusCode, []int{http.StatusNoContent}); err != nil {
		return err

	}
	return nil
}
