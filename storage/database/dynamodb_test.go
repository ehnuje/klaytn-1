package database

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"strings"
	"testing"
	"time"
)

func TestDynamoDB(t *testing.T) {
	dynamo, err := NewDynamoDB(createTestDynamoDBConfig())
	if err != nil {
		t.Fatal(err)
	}
	testRand = rand.New(rand.NewSource(time.Now().UnixNano()))
	testKey := randStrBytes(testRand.Intn(1000))
	val, err := dynamo.Get(testKey)

	assert.Nil(t, val)
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "NoSuchKey"))

	testVal := randStrBytes(500 * 1000)
	assert.NoError(t, dynamo.Put(testKey, testVal))

	returnedVal, returnedErr := dynamo.Get(testKey)
	assert.Equal(t, testVal, returnedVal)
	assert.NoError(t, returnedErr)
}
