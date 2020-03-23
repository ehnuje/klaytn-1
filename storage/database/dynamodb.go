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
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/klaytn/klaytn/log"
)

var dataNotFoundErr = errors.New("data is not found with the given key")

type dynamoDB struct {
	region    string
	endPoint  string
	tableName string

	db *dynamodb.DynamoDB

	logger log.Logger // Contextual logger tracking the database path
}

func NewDynamoDB(endPoint, region, tableName string) (*dynamoDB, error) {
	endPoint = "http://localhost:8000"
	region = "ap-northeast-2"

	session := session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Endpoint: aws.String(endPoint),
			Region:   aws.String(region),
		},
	}))

	db := dynamodb.New(session)

	if _, err := db.DeleteTable(&dynamodb.DeleteTableInput{TableName: &tableName}); err != nil {
		fmt.Println("Error while deleting the table", "tableName", tableName)
	}

	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("Key"),
				AttributeType: aws.String("B"),
			},
			//{
			//	AttributeName: aws.String("Val"),
			//	AttributeType: aws.String("B"),
			//},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("Key"),
				KeyType:       aws.String("HASH"),
			},
			//{
			//	AttributeName: aws.String("Title"),
			//	KeyType:       aws.String("RANGE"),
			//},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(20),
			WriteCapacityUnits: aws.Int64(20),
		},
		TableName: aws.String(tableName),
	}

	_, err := db.CreateTable(input)
	if err != nil {
		fmt.Println("Got error calling CreateTable:")
		fmt.Println(err.Error())
		return nil, err
	}

	return &dynamoDB{
		region:    region,
		endPoint:  endPoint,
		tableName: tableName,
		db:        db,
		logger:    logger.NewWith("region", region, "endPoint", endPoint),
	}, nil
}

func (dynamo *dynamoDB) Type() DBType {
	return DynamoDB
}

// Path returns the path to the database directory.
func (dynamo *dynamoDB) Path() string {
	return fmt.Sprintf("%s-%s", dynamo.region, dynamo.endPoint)
}

type DynamoData struct {
	Key []byte `json:"Key" dynamodbav:"Key"`
	Val []byte `json:"Val" dynamodbav:"Val"`
}

// Put inserts the given key and value pair to the database.
func (dynamo *dynamoDB) Put(key []byte, value []byte) error {
	data := DynamoData{Key: key, Val: value}
	marshaledData, err := dynamodbattribute.MarshalMap(data)
	if err != nil {
		return err
	}

	params := &dynamodb.PutItemInput{
		TableName: aws.String(dynamo.tableName),
		Item:      marshaledData,
	}

	output, err := dynamo.db.PutItem(params)
	if err != nil {
		fmt.Printf("Put ERROR: %v\n", err.Error())
		return err
	}

	fmt.Println(output)
	return nil
}

// Has returns true if the corresponding value to the given key exists.
func (dynamo *dynamoDB) Has(key []byte) (bool, error) {
	if _, err := dynamo.Get(key); err != nil {
		return false, err
	}
	return true, nil
}

// Get returns the corresponding value to the given key if exists.
func (dynamo *dynamoDB) Get(key []byte) ([]byte, error) {
	params := &dynamodb.GetItemInput{
		TableName: aws.String(dynamo.tableName),
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

	var data DynamoData
	if err := dynamodbattribute.UnmarshalMap(result.Item, &data); err != nil {
		return nil, err
	}

	if data.Val == nil {
		return nil, dataNotFoundErr
	}

	return data.Val, nil
}

// Delete deletes the key from the queue and database
func (dynamo *dynamoDB) Delete(key []byte) error {
	params := &dynamodb.DeleteItemInput{
		TableName: aws.String(dynamo.tableName),
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
	dynamo.logger.Info("There's nothing to do when closing DynamoDB")
}

func (dynamo *dynamoDB) NewBatch() Batch {
	return &dynamoBatch{db: dynamo.db, tableName: dynamo.tableName}
}

func (dynamo *dynamoDB) Meter(prefix string) {
}

type dynamoBatch struct {
	db         *dynamodb.DynamoDB
	tableName  string
	batchItems []*dynamodb.WriteRequest
	size       int
}

func (batch *dynamoBatch) Put(key, value []byte) error {
	data := DynamoData{Key: key, Val: value}
	marshaledData, err := dynamodbattribute.MarshalMap(data)
	if err != nil {
		logger.Error("err while batch put", "err", err, "len(value)", len(value))
		return err
	}

	writeRequest := &dynamodb.WriteRequest{
		PutRequest: &dynamodb.PutRequest{Item: marshaledData},
	}
	batch.batchItems = append(batch.batchItems, writeRequest)
	batch.size += len(value)
	return nil
}

func (batch *dynamoBatch) Write() error {
	_, err := batch.db.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			batch.tableName: batch.batchItems,
		},
		ReturnConsumedCapacity:      nil,
		ReturnItemCollectionMetrics: nil,
	})
	return err
}

func (batch *dynamoBatch) ValueSize() int {
	return batch.size
}

func (batch *dynamoBatch) Reset() {
	batch.batchItems = []*dynamodb.WriteRequest{}
	batch.size = 0
}
