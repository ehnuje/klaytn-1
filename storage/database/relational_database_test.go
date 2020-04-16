package database

import (
	"fmt"
	"github.com/jinzhu/gorm"
	"strings"
	"testing"
)

func BenchmarkRelationalDatabase_Put(b *testing.B) {

}

func newTestRelationalDB() (*rdb, error) {
	id := "root"
	password := "root"
	endpoint := fmt.Sprintf("%s:%s@/test", id, password)
	db, err := openMySQL(endpoint)

	if err != nil {
		return nil, err
	}

	if err := resetDB(db); err != nil {
		return nil, err
	}

	err = db.AutoMigrate(&KeyValueModel{}).Error
	if err != nil {
		return nil, err
	}

	return &rdb{db: db, logger: logger.NewWith("", "")}, nil
}

func BenchmarkRelationalDatabase_Batch(b *testing.B) {
	b.StopTimer()

	db, err := newTestRelationalDB()
	if err != nil {
		b.Fatal(err)
	}

	batch := db.NewBatch()

	numItemsAtOnce := 60000

	keys := make([][]byte, numItemsAtOnce)
	vals := make([][]byte, numItemsAtOnce)

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		for i := 0; i < numItemsAtOnce; i++ {
			keys[i] = randStrBytes(32)
			vals[i] = randStrBytes(128)
		}

		b.StartTimer()

		for i := 0; i < numItemsAtOnce; i++ {
			PutAndWriteBatchesOverThreshold(batch, keys[i], vals[i])
		}
		batch.Write()
	}
}

func resetDB(mysql *gorm.DB) error {
	//Drop previous test database if possible.
	if err := mysql.Exec("DROP DATABASE test").Error; err != nil {
		if !strings.Contains(err.Error(), "database doesn't exist") {
			return err
		}
	}
	// Create new test database.
	if err := mysql.Exec("CREATE DATABASE test DEFAULT CHARACTER SET UTF8").Error; err != nil {
		return err
	}
	// Use test database
	if err := mysql.Exec("USE test").Error; err != nil {
		return err
	}
	return nil
}
