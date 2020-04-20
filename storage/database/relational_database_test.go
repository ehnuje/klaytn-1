package database

import (
	"fmt"
	"github.com/jinzhu/gorm"
	"strings"
	"testing"
)

func BenchmarkRelationalDatabase_Put(b *testing.B) {

}

type PerformanceData struct {
	Id        int    `gorm:"column:EVENT_ID;type:bigint(20) unsigned"`
	TimerWait uint64 `gorm:"column:TIMER_WAIT;type:bigint(20) unsigned"`
	SqlText   string `gorm:"column;SQL_TEXT;type:longtext"`
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

	db.Exec("INSERT INTO performance_schema.setup_actors (HOST,USER,ROLE,ENABLED,HISTORY) VALUES('localhost','root','%','YES','YES');")
	db.Exec("UPDATE performance_schema.setup_instruments SET ENABLED = 'YES', TIMED = 'YES' WHERE NAME LIKE '%statement/%';")
	db.Exec("UPDATE performance_schema.setup_instruments SET ENABLED = 'YES', TIMED = 'YES' WHERE NAME LIKE '%stage/%';")

	db.Exec("UPDATE performance_schema.setup_consumers SET ENABLED = 'YES' WHERE NAME LIKE '%events_statements_%';")
	db.Exec("UPDATE performance_schema.setup_consumers SET ENABLED = 'YES' WHERE NAME LIKE '%events_stages_%';")

	db.Exec("SELECT * from test.key_value_models")

	var data PerformanceData
	db.Raw("SELECT EVENT_ID, TRUNCATE(TIMER_WAIT/1000000000000,6) as Duration, SQL_TEXT FROM performance_schema.events_statements_history_long where SQL_TEXT is not null;").Scan(&data)

	fmt.Println(data)
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
