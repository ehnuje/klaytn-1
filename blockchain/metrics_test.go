package blockchain

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/rcrowley/go-metrics"
)

var pv = []float64{0.5, 0.75, 0.95, 0.99, 0.999, 0.9999}
var pv_str = []string{"_0_5", "_0_75", "_0_95", "_0_99", "_0_999", "_0_9999"}

type testOption struct {
	id               int
	updateIteration  int
	updateInterval   time.Duration
	normalVal        time.Duration
	abnormalVal      time.Duration
	abnormalInterval int
}

func metricTest(to testOption, wg sync.WaitGroup) {
	ticker := time.NewTicker(to.updateInterval)
	// update the metrics every 1 second and there is no delay

	meter := metrics.NewMeter()
	timer := metrics.NewTimer()
	i := 0

	for {
		select {
		case <-ticker.C:
			if i >= to.updateIteration {
				return
			}
			isAbnormal := i%to.abnormalInterval == 0
			if isAbnormal {
				meter.Mark(int64(to.abnormalVal))
				timer.Update(to.abnormalVal)
			} else {
				meter.Mark(int64(to.normalVal))
				timer.Update(to.normalVal)
			}
			fmt.Println("test"+strconv.Itoa(to.id), "i", i,
				"meter.Rate1()", meter.Rate1(), "meter.Rate5()", meter.Rate5(), "meter.Rate15()", meter.Rate15(),
				"timer.Rate1()", timer.Rate1(), "timer.Rate5()", timer.Rate5(), "timer.Rate15()", timer.Rate15(),
				"timer.Mean()", timer.Mean(), "timer_0.9999", timer.Percentile(0.9999))
			i++
		}
	}

}

func TestMetric(t *testing.T) {
	tos := []testOption{
		// different abnormal val
		{11, 1000, 1000 * time.Millisecond, 100 * time.Millisecond, 500 * time.Millisecond, 10},
		{12, 1000, 1000 * time.Millisecond, 100 * time.Millisecond, 1000 * time.Millisecond, 10},
		{13, 1000, 1000 * time.Millisecond, 100 * time.Millisecond, 2000 * time.Millisecond, 10},

		// diff updateInterval
		{21, 1000, 500 * time.Millisecond, 100 * time.Millisecond, 500 * time.Millisecond, 10},
		{22, 1000, 1000 * time.Millisecond, 100 * time.Millisecond, 500 * time.Millisecond, 10},
		{23, 1000, 2000 * time.Millisecond, 100 * time.Millisecond, 500 * time.Millisecond, 10},
	}
	wg := sync.WaitGroup{}
	wg.Add(len(tos))
	for _, to := range tos {
		go metricTest(to, wg)
	}

	wg.Wait()
}
