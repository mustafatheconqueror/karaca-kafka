package karaca_kafka

const (
	MainSuffix  = ".main"
	RetrySuffix = "_retry"
	ErrorSuffix = "_error"
	DeadSuffix  = "_dead"

	AcknowledgeTypeAll    = "all"
	AcknowledgeTypeNone   = "none"
	AcknowledgeTypeLeader = "leader"

	CompressionTypeGzip   = "gzip"
	CompressionTypeSnappy = "snappy"
	CompressionTypeNone   = "none"

	AutoOffsetResetTypeEarliest string = "earliest"
	AutoOffsetResetTypeLatest   string = "latest"

	DefaultDeliveryTimeoutMs = 10 // in seconds

	MainTopicPartitionCount    = 3
	MainTopicReplicationFactor = 3
	MainTopicRetention         = RetentionOneWeek

	RetryTopicPartitionCount    = 3
	RetryTopicReplicationFactor = 3
	RetryTopicRetention         = RetentionOneMinute * 2

	ErrorTopicPartitionCount    = 3
	ErrorTopicReplicationFactor = 3
	ErrorTopicRetention         = RetentionOneMonth

	DeadTopicPartitionCount    = 3
	DeadTopicReplicationFactor = 3
	DeadTopicRetention         = RetentionOneMonth

	RetentionOneDay    = 86400000
	RetentionOneMinute = 6000
	RetentionOneWeek   = RetentionOneDay * 7
	RetentionOneMonth  = RetentionOneWeek * 30

	MinInSyncReplicas = "2"
)
