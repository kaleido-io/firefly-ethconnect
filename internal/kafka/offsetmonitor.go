// Copyright 2021 Kaleido

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

type OffsetMonitor interface {
	UpdateInflightEstimate(msgSize int64) (inflightEstimate int64)
}

type offsetMonitor struct {
	client            sarama.Client
	offsetManager     sarama.OffsetManager
	consumer          sarama.Consumer
	topic             string
	consumerGroup     string
	interval          time.Duration
	totalSizeNotified int64
	countNotified     int64
	averageSize       int64
	mux               sync.Mutex
	largestGap        int64
}

func newOffsetMonitor(client sarama.Client, topic, consumerGroup string, initialSize int64, interval time.Duration) (om *offsetMonitor, err error) {
	om = &offsetMonitor{
		client:            client,
		topic:             topic,
		consumerGroup:     consumerGroup,
		totalSizeNotified: initialSize,
		countNotified:     1,
		averageSize:       initialSize,
		interval:          interval,
	}
	if om.offsetManager, err = sarama.NewOffsetManagerFromClient(om.consumerGroup, om.client); err != nil {
		return nil, err
	}
	if om.consumer, err = sarama.NewConsumerFromClient(om.client); err != nil {
		return nil, err
	}
	go om.pollingLoop()
	return om, nil
}

func (om *offsetMonitor) pollingLoop() {
	for {

		partitions, err := om.consumer.Partitions(om.topic)
		if err != nil {
			log.Warnf("Failed to query partitions for topic '%s': %s", om.topic, err)
		}

		if err == nil && len(partitions) > 0 {
			var newLargestGap int64 = -1
			var totalInFlight int64 = 0
			for partition := range partitions {
				gap, err := om.calculateGap(partition)
				if err != nil {
					log.Warnf("Failed to calculate gap for partition %d of topic '%s': %s", partition, om.topic, err)
					newLargestGap = -1 // do not update
					break              // do not attempt to query other partitions
				} else {
					totalInFlight += gap
					if gap > newLargestGap {
						newLargestGap = gap
					}
				}
			}
			if newLargestGap >= 0 {
				log.Warnf("Topic '%s' totalInflight=%d partitions=%d largestGap=%d", om.topic, totalInFlight, len(partitions), newLargestGap)
				om.largestGap = newLargestGap
			}
		}

		time.Sleep(om.interval)
	}
}

func (om *offsetMonitor) calculateGap(partition int) (int64, error) {

}

func (om *offsetMonitor) UpdateInflightEstimate(msgSize int64) (inflightEstimate int64) {
	om.mux.Lock()
	defer om.mux.Unlock()

	om.totalSizeNotified += msgSize
	om.countNotified++
	om.averageSize = om.totalSizeNotified / om.countNotified

	return om.averageSize * om.largestGap
}
