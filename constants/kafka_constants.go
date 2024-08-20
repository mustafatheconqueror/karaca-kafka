package constants

// Acknowledge types define the levels of acknowledgment required from the broker.
const (
	// AcknowledgeTypeAll requires all replicas to acknowledge the record.
	AcknowledgeTypeAll = "all"

	// AcknowledgeTypeNone does not require any acknowledgment.
	AcknowledgeTypeNone = "none"

	// AcknowledgeTypeLeader requires only the leader replica to acknowledge the record.
	AcknowledgeTypeLeader = "leader"
)

// Compression types define the compression algorithms used for Kafka messages.
const (
	// CompressionTypeGzip uses gzip compression for the Kafka messages.
	CompressionTypeGzip = "gzip"

	// CompressionTypeSnappy uses Snappy compression for the Kafka messages.
	CompressionTypeSnappy = "snappy"

	// CompressionTypeNone does not compress Kafka messages.
	CompressionTypeNone = "none"
)

// Default settings for Kafka producer configuration.
const (
	// DefaultDeliveryTimeoutMs specifies the default delivery timeout in milliseconds.
	DefaultDeliveryTimeoutMs = 10 // in seconds
)

const (
	AutoOffsetResetTypeEarliest string = "earliest"
	AutoOffsetResetTypeLatest   string = "latest"
)
