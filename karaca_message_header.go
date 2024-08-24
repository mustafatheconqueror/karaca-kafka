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

	const (
		retryCountKey  = "retryCount"
		isRetryableKey = "isRetryable"
	)

	// Eğer mesajda header yoksa, retryCount ve isRetryable header'larını ekle
	if message.Headers == nil {
		message.Headers = []kafka.Header{
			{Key: retryCountKey, Value: []byte("1")},
			{Key: isRetryableKey, Value: []byte{1}},
		}
		return
	}

	isRetryable := false
	retryCountIndex := -1

	// Header'ları tarayarak isRetryable ve retryCount değerlerini kontrol et
	for i, header := range message.Headers {
		switch header.Key {
		case isRetryableKey:
			if string(header.Value) == "true" || (len(header.Value) == 1 && header.Value[0] == 1) {
				isRetryable = true
			}
		case retryCountKey:
			retryCountIndex = i
		}
	}

	// Eğer isRetryable true ise ve retryCount varsa, retryCount'u artır
	if isRetryable {
		if retryCountIndex != -1 {
			retryCount, _ := strconv.Atoi(string(message.Headers[retryCountIndex].Value))
			message.Headers[retryCountIndex].Value = []byte(strconv.Itoa(retryCount + 1))
		} else {
			// retryCount header'ı yoksa, yeni bir retryCount ekle
			message.Headers = append(message.Headers, kafka.Header{
				Key:   retryCountKey,
				Value: []byte("1"),
			})
		}
	}
}
