package constants

const (
	AcknowledgeTypeAll    = "all"
	AcknowledgeTypeNone   = "none"
	AcknowledgeTypeLeader = "leader"

	CompressionTypeGzip   = "gzip"
	CompressionTypeSnappy = "snappy"
	CompressionTypeNone   = "none"

	AutoOffsetResetTypeEarliest string = "earliest"
	AutoOffsetResetTypeLatest   string = "latest"

	DefaultDeliveryTimeoutMs = 10 // in seconds
)
