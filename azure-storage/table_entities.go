package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

const (
	partitionKeyNode = "PartitionKey"
	rowKeyNode       = "RowKey"
)

// TableEntity struct specifies entity to be saved to Azure Tables
type TableEntity struct {
	PartitionKey string
	RowKey       string
	Fields       map[string]interface{}
}

// InsertEntity inserts an entity in the specified table.
// The function fails if there is an entity with the same PartitionKey and RowKey in the table.
func (c *TableServiceClient) InsertEntity(table AzureTable, entity TableEntity) error {
	var err error

	if statusCode, err := c.execTable(table, entity, false, "POST"); err != nil {
		return checkRespCode(statusCode, []int{http.StatusCreated})
	}

	return err
}

func (c *TableServiceClient) execTable(table AzureTable, entity TableEntity, specifyKeysInURL bool, method string) (int, error) {
	uri := c.client.getEndpoint(tableServiceName, pathForTable(table), url.Values{})
	if specifyKeysInURL {
		uri += fmt.Sprintf("(PartitionKey='%s',RowKey='%s')", url.QueryEscape(entity.PartitionKey), url.QueryEscape(entity.RowKey))
	}

	headers := c.getStandardHeaders()

	var buf *bytes.Buffer
	var err error
	if buf, err = serializeEntity(entity); err != nil {
		return 0, err
	}

	headers["Content-Length"] = fmt.Sprintf("%d", buf.Len())

	resp, err := c.client.execTable(method, uri, headers, buf)
	if err != nil {
		return 0, err
	}
	defer resp.body.Close()

	return resp.statusCode, nil
}

func serializeEntity(entity TableEntity) (*bytes.Buffer, error) {
	request := make(map[string]interface{})
	for k, v := range entity.Fields {
		request[k] = v
	}

	// Inject PartitionKey and RowKey
	request[partitionKeyNode] = entity.PartitionKey
	request[rowKeyNode] = entity.RowKey

	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(&request); err != nil {
		return nil, err
	}

	return buf, nil
}
