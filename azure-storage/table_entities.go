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
	statusCode, err := c.execTable(table, entity, "POST")
	if err != nil {
		return checkRespCode(statusCode, []int{http.StatusCreated})
	}
	return nil
}

// BatchInsert inserts set of entities in the specified table.
// Function assumes that batch is formed properly
func (c *TableServiceClient) BatchInsert(table AzureTable, entities []*TableEntity) error {
	uri := c.client.getEndpoint(tableServiceName, pathForTable("$batch"), url.Values{})
	uuid, err := pseudoUUID()
	if err != nil {
		return err
	}
	boundary := "batch_" + uuid
	headers := map[string]string{
		"x-ms-version":          "2015-02-21",
		"x-ms-date":             currentTimeRfc1123Formatted(),
		"Accept-Charset":        "UTF-8",
		"Content-Type":          "multipart/mixed; boundary=" + boundary,
		"DataServiceVersion":    "3.0;",
		"MaxDataServiceVersion": "3.0;NetFx",
	}
	content, err := buildBatchContent(c, boundary, table, entities)
	if err != nil {
		return err
	}
	headers["Content-Length"] = fmt.Sprintf("%d", content.Len())

	resp, err := c.client.execTable("POST", uri, headers, content)
	if err != nil {
		return err
	}
	defer resp.body.Close()

	return nil
}

func buildBatchContent(c *TableServiceClient, boundary string, table AzureTable, entities []*TableEntity) (*bytes.Buffer, error) {
	uuid, err := pseudoUUID()
	if err != nil {
		return nil, err
	}
	changeset := "changeset_" + uuid

	var buffer bytes.Buffer

	buffer.WriteString("--")
	buffer.WriteString(boundary)
	buffer.WriteString("\nContent-Type: multipart/mixed; boundary=")
	buffer.WriteString(changeset)
	buffer.WriteString("\n")

	uri := c.client.getEndpoint(tableServiceName, pathForTable(table), url.Values{})
	for _, entity := range entities {
		buffer.WriteString("--")
		buffer.WriteString(changeset)
		buffer.WriteString("\nContent-Type: application/http\nContent-Transfer-Encoding: binary\n\nPOST ")
		buffer.WriteString(uri)
		buffer.WriteString(" HTTP/1.1\nContent-Type: application/json\nAccept: application/json;odata=minimalmetadata\n")
		buffer.WriteString("Prefer: return-no-content\nDataServiceVersion: 3.0;\n\n")

		serializedEntity, err := serializeEntity(*entity)
		if err != nil {
			return nil, err
		}
		buffer.Write(serializedEntity.Bytes())
		buffer.WriteString("\n")
	}

	buffer.WriteString("\n")
	buffer.WriteString("--")
	buffer.WriteString(changeset)
	buffer.WriteString("--\n--")
	buffer.WriteString(boundary)
	buffer.WriteString("--")

	return &buffer, nil
}

func (c *TableServiceClient) execTable(table AzureTable, entity TableEntity, method string) (int, error) {
	uri := c.client.getEndpoint(tableServiceName, pathForTable(table), url.Values{})
	headers := c.getStandardHeaders()
	buf, err := serializeEntity(entity)
	if err != nil {
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
