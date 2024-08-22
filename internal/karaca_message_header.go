package internal

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"strconv"
	"time"
)

type KafkaHeaders struct {
	TimeStamp    time.Time
	UserName     string
	IdentityName string
	IdentityType string
	Version      int
	MessageType  string
}

func MapHeaders(headers []kafka.Header) KafkaHeaders {
	version, _ := strconv.Atoi(Version(headers))
	timeStamp, _ := time.Parse("01/02/2006 15:04:05", TimeStamp(headers))

	return KafkaHeaders{
		IdentityName: IdentityName(headers),
		IdentityType: IdentityType(headers),
		Version:      version,
		UserName:     UserName(headers),
		TimeStamp:    timeStamp,
		MessageType:  EventType(headers),
	}
}

func IdentityName(headers []kafka.Header) string {
	return string(getHeaderValue(headers, "identityName"))
}

func IdentityType(headers []kafka.Header) string {
	return string(getHeaderValue(headers, "identityType"))
}
func CorrelationId(headers []kafka.Header) string {
	return string(getHeaderValue(headers, "correlationId"))
}

func UserName(headers []kafka.Header) string {
	return string(getHeaderValue(headers, "userName"))
}

func EventType(headers []kafka.Header) string {
	return string(getHeaderValue(headers, "type"))
}

func Version(headers []kafka.Header) string {
	return string(getHeaderValue(headers, "version"))
}

func Tenant(headers []kafka.Header) string {
	return string(getHeaderValue(headers, "tenant"))
}

func Hash(headers []kafka.Header) string {
	return string(getHeaderValue(headers, "hash"))
}

func RetryCount(headers []kafka.Header) int {
	retryCount, _ := strconv.Atoi(string(getHeaderValue(headers, "retryCount")))
	return retryCount
}

func TimeStamp(headers []kafka.Header) string {
	return string(getHeaderValue(headers, "timeStamp"))
}

func getHeaderValue(headers []kafka.Header, key string) []byte {
	for _, header := range headers {
		if header.Key == key {
			return header.Value
		}
	}
	return []byte{}
}

func IncrementRetryCount(message *kafka.Message) {

	isFoundRetryHeader := false

	if message.Headers == nil {
		message.Headers = make([]kafka.Header, 0)

		message.Headers = append(message.Headers, kafka.Header{
			Key:   "retryCount",
			Value: []byte("1"),
		})

		return
	}

	for i, header := range message.Headers {
		if header.Key == "retryCount" {
			isFoundRetryHeader = true
			retryCount, _ := strconv.Atoi(string(header.Value))
			message.Headers[i].Value = []byte(strconv.Itoa(retryCount + 1))
			return
		}
	}

	if !isFoundRetryHeader {
		message.Headers = append(message.Headers, kafka.Header{
			Key:   "retryCount",
			Value: []byte("1"),
		})
	}
}
