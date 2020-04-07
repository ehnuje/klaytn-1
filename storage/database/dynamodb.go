// Copyright 20 The klaytn Authors
// This file is part of the klaytn library.
//
// The klaytn library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The klaytn library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the klaytn library. If not, see <http://www.gnu.org/licenses/>.

package database

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/klaytn/klaytn/log"
	"math"
	"time"
)

var dataNotFoundErr = errors.New("data is not found with the given key")
var nilDynamoConfigErr = errors.New("attempt to create DynamoDB with nil configuration")

type DynamoDBConfig struct {
	Region             string
	Endpoint           string
	TableName          string
	ReadCapacityUnits  int64
	WriteCapacityUnits int64
}

const dynamoItemSizeLimit = 400 * 1024

/*
 * Please Run DynamoDB local with docker
 * $ docker pull amazon/dynamodb-local
 * $ docker run -d -p 8000:8000 amazon/dynamodb-local
 */
func createTestDynamoDBConfig() *DynamoDBConfig {
	return &DynamoDBConfig{
		Region:             "ap-northeast-2",
		Endpoint:           "https://dynamodb.ap-northeast-2.amazonaws.com", //"http://localhost:4569",  "https://dynamodb.ap-northeast-2.amazonaws.com"
		TableName:          "kas-test-melvin-555",                           // + strconv.Itoa(time.Now().Nanosecond()),
		ReadCapacityUnits:  100,
		WriteCapacityUnits: 100,
	}
}

type dynamoDB struct {
	config *DynamoDBConfig
	db     *dynamodb.DynamoDB
	fdb    fileDB
	logger log.Logger // Contextual logger tracking the database path

	internalBatch Batch
}

func NewDynamoDB(config *DynamoDBConfig) (*dynamoDB, error) {
	if config == nil {
		return nil, nilDynamoConfigErr
	}

	session := session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Endpoint: aws.String(config.Endpoint),
			Region:   aws.String(config.Region),
		},
	}))

	db := dynamodb.New(session)
	dynamoDB := &dynamoDB{
		config: config,
		db:     db,
		logger: logger.NewWith("region", config.Region, "endPoint", config.Endpoint),
	}

	// Check if the table exists or not
	tableExists, err := dynamoDB.hasTable()
	if err != nil {
		dynamoDB.logger.Error("")
		return nil, err
	}

	s3FileDB, err := newS3FileDB("ap-northeast-2", "http://localhost:4572", "kas-test-bucket-12345")
	if err != nil {
		dynamoDB.logger.Error("")
		return nil, err
	}

	dynamoDB.fdb = s3FileDB

	dynamoDB.internalBatch = dynamoDB.NewBatch()

	// Table already exists, return here without doing anything
	if tableExists {
		dynamoDB.logger.Info("")
		return dynamoDB, nil
	}

	//// Table does not exist, create one here
	//if err := dynamoDB.createTable(); err != nil {
	//	dynamoDB.logger.Error("")
	//	return nil, err
	//}

	dynamoDB.logger.Info("")
	return dynamoDB, nil
}

func (dynamo *dynamoDB) createTable() error {
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("Key"),
				AttributeType: aws.String("B"), // B - the attribute is of type Binary
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("Key"),
				KeyType:       aws.String("HASH"), // HASH - partition key, RANGE - sort key
			},
			//{
			//	AttributeName: aws.String("Title"),
			//	KeyType:       aws.String("RANGE"),
			//},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(dynamo.config.ReadCapacityUnits),
			WriteCapacityUnits: aws.Int64(dynamo.config.WriteCapacityUnits),
		},
		TableName: aws.String(dynamo.config.TableName),
	}

	_, err := dynamo.db.CreateTable(input)
	if err != nil {
		dynamo.logger.Error("Error while creating the DynamoDB table", "tableName", dynamo.config.TableName)
		return err
	}
	dynamo.logger.Info("Successfully created the Dynamo table", "tableName", dynamo.config.TableName)
	return nil
}

func (dynamo *dynamoDB) deleteTable() error {
	if _, err := dynamo.db.DeleteTable(&dynamodb.DeleteTableInput{TableName: &dynamo.config.TableName}); err != nil {
		dynamo.logger.Error("Error while deleting the DynamoDB table", "tableName", dynamo.config.TableName)
		return err
	}
	dynamo.logger.Info("Successfully deleted the DynamoDB table", "tableName", dynamo.config.TableName)
	return nil
}

func (dynamo *dynamoDB) hasTable() (bool, error) {
	listTableLimit := int64(100)
	output, err := dynamo.db.ListTables(&dynamodb.ListTablesInput{
		ExclusiveStartTableName: &dynamo.config.TableName,
		Limit:                   &listTableLimit,
	})
	if err != nil {
		return false, err
	}

	if len(output.TableNames) == 0 {
		return false, nil
	}

	for _, tableName := range output.TableNames {
		if *tableName == dynamo.config.TableName {
			return true, nil
		}
	}
	return false, nil
}

func (dynamo *dynamoDB) Type() DBType {
	return DynamoDB
}

// Path returns the path to the database directory.
func (dynamo *dynamoDB) Path() string {
	return fmt.Sprintf("%s-%s", dynamo.config.Region, dynamo.config.Endpoint)
}

type DynamoData struct {
	Key []byte `json:"Key" dynamodbav:"Key"`
	Val []byte `json:"Val" dynamodbav:"Val"`
}

// Put inserts the given key and value pair to the database.
func (dynamo *dynamoDB) Put(key []byte, val []byte) error {
	//start := time.Now()
	//defer logger.Info("", "keySize", len(key), "valSize", len(val),
	//	 "putElapsedTime", time.Since(start))
	//return dynamo.internalBatch.Put(key, val)

	if len(val) > dynamoItemSizeLimit {
		_, err := dynamo.fdb.write(item{key: key, val: val})
		if err != nil {
			return err
		}

		return dynamo.Put(key, overSizedDataPrefix)
	}

	data := DynamoData{Key: key, Val: val}
	marshaledData, err := dynamodbattribute.MarshalMap(data)
	if err != nil {
		return err
	}

	params := &dynamodb.PutItemInput{
		TableName: aws.String(dynamo.config.TableName),
		Item:      marshaledData,
	}

	//marshalTime := time.Since(start)
	_, err = dynamo.db.PutItem(params)
	if err != nil {
		fmt.Printf("Put ERROR: %v\n", err.Error())
		return err
	}

	return nil
}

func (dynamo *dynamoDB) WriteInternalBatch() {
	dynamo.internalBatch.Write()
	dynamo.internalBatch.Reset()
}

// Has returns true if the corresponding value to the given key exists.
func (dynamo *dynamoDB) Has(key []byte) (bool, error) {
	if _, err := dynamo.Get(key); err != nil {
		return false, err
	}
	return true, nil
}

var overSizedDataPrefix = []byte("oversizeditem")

// Get returns the corresponding value to the given key if exists.
func (dynamo *dynamoDB) Get(key []byte) ([]byte, error) {

	//getStart := time.Now()
	//defer func() {logger.Error("Get", "elapsed", time.Since(getStart))}()

	params := &dynamodb.GetItemInput{
		TableName: aws.String(dynamo.config.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			"Key": {
				B: key,
			},
		},
	}

	result, err := dynamo.db.GetItem(params)
	if err != nil {
		fmt.Printf("Get ERROR: %v\n", err.Error())
		return nil, err
	}

	if result.Item == nil {
		return nil, dataNotFoundErr
		//return dynamo.fdb.read(key)
	}

	var data DynamoData
	if err := dynamodbattribute.UnmarshalMap(result.Item, &data); err != nil {
		return nil, err
	}

	if data.Val == nil {
		return nil, dataNotFoundErr
	}

	if !bytes.Equal(data.Val, overSizedDataPrefix) {
		return data.Val, nil
	} else {
		return dynamo.fdb.read(key)
	}
}

// Delete deletes the key from the queue and database
func (dynamo *dynamoDB) Delete(key []byte) error {
	params := &dynamodb.DeleteItemInput{
		TableName: aws.String(dynamo.config.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			"Key": {
				B: key,
			},
		},
	}

	result, err := dynamo.db.DeleteItem(params)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err.Error())
		return err
	}
	fmt.Println(result)
	return nil
}

func (dynamo *dynamoDB) Close() {
	if err := dynamo.internalBatch.Write(); err != nil {
		logger.Error("", "err", err)
	}

	dynamo.logger.Info("There's nothing to do when closing DynamoDB")
}

func (dynamo *dynamoDB) NewBatch() Batch {
	return &dynamoBatch{db: dynamo, tableName: dynamo.config.TableName}
}

func (dynamo *dynamoDB) Meter(prefix string) {
}

type dynamoBatch struct {
	db                 *dynamoDB
	tableName          string
	batchItems         []*dynamodb.WriteRequest
	oversizeBatchItems []item
	size               int
}

func (batch *dynamoBatch) Put(key, val []byte) error {
	// If the size of the item is larger than the limit, it should be handled in different way
	if len(val) > dynamoItemSizeLimit {
		batch.oversizeBatchItems = append(batch.oversizeBatchItems, item{key: key, val: val})
		batch.size += len(val)
		return nil
	}

	data := DynamoData{Key: key, Val: val}
	marshaledData, err := dynamodbattribute.MarshalMap(data)
	if err != nil {
		logger.Error("err while batch put", "err", err, "len(val)", len(val))
		return err
	}

	writeRequest := &dynamodb.WriteRequest{
		PutRequest: &dynamodb.PutRequest{Item: marshaledData},
	}
	batch.batchItems = append(batch.batchItems, writeRequest)
	batch.size += len(val)
	return nil
}

func (batch *dynamoBatch) Write() error {
	if batch.size == 0 {
		return nil
	}

	start := time.Now()

	writtenItems := 0
	totalItems := len(batch.batchItems)

	errCh := make(chan error, totalItems/25+len(batch.oversizeBatchItems)+1)
	numErrsToCheck := 0

	logger.Info("", "errChSize", cap(errCh))

	for writtenItems < totalItems {
		thisTime := int(math.Min(25.0, float64(totalItems-writtenItems)))
		thisTimeItems := batch.batchItems[writtenItems : writtenItems+thisTime]

		go func(bis []*dynamodb.WriteRequest) {
			_, err := batch.db.db.BatchWriteItem(&dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]*dynamodb.WriteRequest{
					batch.tableName: bis,
				},
				ReturnConsumedCapacity:      nil,
				ReturnItemCollectionMetrics: nil,
			})
			errCh <- err
			numErrsToCheck++
		}(thisTimeItems)

		writtenItems += thisTime
	}

	for _, item := range batch.oversizeBatchItems {
		go func() {
			_, err := batch.db.fdb.write(item)
			if err == nil {
				if err2 := batch.db.Put(item.key, overSizedDataPrefix); err2 != nil {
					errCh <- err2
					numErrsToCheck++
				}
			}
			errCh <- err
			numErrsToCheck++
		}()
	}

	for i := 0; i < numErrsToCheck; i++ {
		if err := <-errCh; err != nil {
			return err
		}
	}

	logger.Info("batch write time 22222", "elapsedTime", time.Since(start))
	return nil
}

func (batch *dynamoBatch) ValueSize() int {
	return batch.size
}

func (batch *dynamoBatch) Reset() {
	batch.batchItems = []*dynamodb.WriteRequest{}
	batch.oversizeBatchItems = []item{}
	batch.size = 0
}
