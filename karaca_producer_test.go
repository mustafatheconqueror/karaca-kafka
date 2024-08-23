package karaca_kafka

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
	"time"
)

//Test Cases
/*
case 3: başarılı şekilde broker'a bağlansın ama başarılı produce edemesin
case 4: başararılı şekilde broker'a bağlansın ve başarılı produce etsin.
case 5: başarılı şekilde producer'a bağlansın ve başarılı şekilde connection close etsin
case 6: başarıl şekilde producer'a bağlansın ama başarılı şekilde connection close edemesin
*/

func TestNewKaracaProducer_CannotConnectToBroker(t *testing.T) {
	producerConfig := ProducerConfig{
		Brokers:           []string{"invalid-broker:9092"},
		AcknowledgeType:   "all",
		CompressionType:   "gzip",
		DeliveryTimeoutMs: 10 * time.Second,
	}

	producer, err := NewKaracaProducer(producerConfig)
	assert.NotNil(t, producer, "Producer should not be nil even if connection fails")
	assert.NoError(t, err, "Expected no immediate error from NewKaracaProducer")

	errChan := make(chan string, 1)

	// Set up the event listener to capture errors asynchronously
	go func() {
		for e := range producer.ListenEvents() {
			if e.String() == "1/1 brokers are down" {
				errChan <- e.String()
				return
			}
		}
	}()

	// Increase the sleep duration to allow more time for errors to propagate
	time.Sleep(3 * time.Second)

	select {
	case errMsg := <-errChan:
		assert.Equal(t, "1/1 brokers are down", errMsg, "Expected an asynchronous error due to connection failure")
	default:
		assert.Fail(t, "Expected an error but none was received")
	}

	// Clean up
	_ = producer.Close()
}

func TestNewKaracaProducer_CanConnectToBroker(t *testing.T) {

	producerConfig := ProducerConfig{
		Brokers:           []string{"localhost:9092", "localhost:9093", "localhost:9094"},
		AcknowledgeType:   "all",
		CompressionType:   "gzip",
		DeliveryTimeoutMs: 10 * time.Second,
	}

	producer, err := NewKaracaProducer(producerConfig)
	assert.NotNil(t, producer, "Producer should not be nil even if connection fails")
	assert.NoError(t, err, "Expected no immediate error from NewKaracaProducer")

	errChan := make(chan string, 1)

	// Set up the event listener to capture errors asynchronously
	go func() {
		for e := range producer.ListenEvents() {
			log.Println(e.String())
			if e.String() == "3/3 brokers are down" {
				errChan <- e.String()
				return
			}
		}
	}()

	// Increase the sleep duration to allow more time for errors to propagate
	time.Sleep(3 * time.Second)

	select {
	case errMsg := <-errChan:
		assert.Fail(t, fmt.Sprintf("Unexpected asynchronous error: %v", errMsg))
	default:
		log.Println("Connection to brokers was successful.")
	}

	// Clean up
	_ = producer.Close()
}
