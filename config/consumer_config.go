package config

type ConsumerConfig struct {
	Brokers             []string
	AppName             string
	Topics              []string
	AutoOffsetResetType string
}
