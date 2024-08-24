package karaca_kafka

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"strconv"
	"time"
)

type KaracaMessageHeader struct {
	TimeStamp    time.Time
	UserName     string
	IdentityName string
	IdentityType string
	Version      int
	MessageType  string
	IsRetryable  bool
}

func mapHeaders(headers []kafka.Header) KaracaMessageHeader {
	version, _ := strconv.Atoi(Version(headers))
	timeStamp, _ := time.Parse("01/02/2006 15:04:05", TimeStamp(headers))

	return KaracaMessageHeader{
		IdentityName: IdentityName(headers),
		IdentityType: IdentityType(headers),
		Version:      version,
		UserName:     UserName(headers),
		TimeStamp:    timeStamp,
		MessageType:  EventType(headers),
		IsRetryable:  IsRetryable(headers),
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

func IsRetryable(headers []kafka.Header) bool {

	value := getHeaderValue(headers, "isRetryable")
	if len(value) == 0 {
		return false
	}
	return value[0] == 1
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
