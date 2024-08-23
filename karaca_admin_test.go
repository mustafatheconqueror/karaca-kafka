package karaca_kafka

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// Sonuç güvenilir değil çünkü async mesaj dinleme yapısı var o sebeple karaca_producerdaki gibi bir yapı inşa etmen gerekiyor.
func TestKaracaAdmin_Close(t *testing.T) {

	producerConfig := ProducerConfig{
		Brokers:           []string{"localhost:9092"},
		AcknowledgeType:   "all",
		CompressionType:   "gzip",
		DeliveryTimeoutMs: 10 * time.Second,
	}

	producer, err := NewKaracaProducer(producerConfig)
	assert.NoError(t, err, "Producer creation should not fail")
	assert.NotNil(t, producer, "Producer should not be nil")

	admin, err := NewAdminFromKaracaProducer(producer)
	assert.NoError(t, err, "Admin creation should not fail")
	assert.NotNil(t, admin, "Admin should not be nil")

	admin.Close()

	_ = producer.Close()
}
