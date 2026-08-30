package dynamodbui

// TableSummary is the UI representation of a DynamoDB table listing.
type TableSummary struct {
	Name        string `json:"name"`
	Status      string `json:"status"`
	ItemCount   int64  `json:"itemCount"`
	SizeBytes   int64  `json:"sizeBytes"`
	CreatedAt   string `json:"createdAt,omitempty"`
	BillingMode string `json:"billingMode"`
	ARN         string `json:"arn,omitempty"`
}

// ListTablesResponse is the response for GET /dynamodb/tables.
type ListTablesResponse struct {
	Items         []TableSummary `json:"items"`
	LastTableName string         `json:"lastTableName,omitempty"`
	Total         int            `json:"total"`
}

// KeySchemaElement describes a table/index key.
type KeySchemaElement struct {
	AttributeName string `json:"attributeName"`
	KeyType       string `json:"keyType"` // "HASH" | "RANGE"
}

// AttributeDefinition describes a table attribute.
type AttributeDefinition struct {
	AttributeName string `json:"attributeName"`
	AttributeType string `json:"attributeType"` // "S" | "N" | "B"
}

// CreateTableRequest is the body for POST /dynamodb/tables.
type CreateTableRequest struct {
	TableName            string                `json:"tableName"`
	KeySchema            []KeySchemaElement    `json:"keySchema"`
	AttributeDefinitions []AttributeDefinition `json:"attributeDefinitions"`
	BillingMode          string                `json:"billingMode"` // "PAY_PER_REQUEST" | "PROVISIONED"
	ReadCapacity         int                   `json:"readCapacity,omitempty"`
	WriteCapacity        int                   `json:"writeCapacity,omitempty"`
}

// ScanResponse is the response for GET /dynamodb/tables/{table}/scan.
type ScanResponse struct {
	Items            []map[string]any `json:"items"`
	Count            int              `json:"count"`
	ScannedCount     int              `json:"scannedCount"`
	LastEvaluatedKey map[string]any   `json:"lastEvaluatedKey,omitempty"`
}

// QueryRequest is the body for POST /dynamodb/tables/{table}/query.
type QueryRequest struct {
	KeyConditionExpression    string            `json:"keyConditionExpression"`
	FilterExpression          string            `json:"filterExpression,omitempty"`
	ExpressionAttributeNames  map[string]string `json:"expressionAttributeNames,omitempty"`
	ExpressionAttributeValues map[string]any    `json:"expressionAttributeValues,omitempty"`
	IndexName                 string            `json:"indexName,omitempty"`
	Limit                     int               `json:"limit,omitempty"`
	ScanIndexForward          *bool             `json:"scanIndexForward,omitempty"`
	ExclusiveStartKey         map[string]any    `json:"exclusiveStartKey,omitempty"`
}

// PutItemRequest is the body for POST /dynamodb/tables/{table}/items.
type PutItemRequest struct {
	Item map[string]any `json:"item"`
}

// GetItemRequest is the body for POST /dynamodb/tables/{table}/items/get.
type GetItemRequest struct {
	Key map[string]any `json:"key"`
}

// DeleteItemRequest is the body for DELETE /dynamodb/tables/{table}/items.
type DeleteItemRequest struct {
	Key map[string]any `json:"key"`
}
