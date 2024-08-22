package internal

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/mustafatheconqueror/karaca-kafka/internal/constants"
	constants2 "github.com/mustafatheconqueror/karaca-kafka/pkg/constants"
	karacakafka "github.com/mustafatheconqueror/karaca-kafka/pkg/kafka"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type karacaConsumer struct {
	context.Context
	AdminClient    *kafka.AdminClient
	Producer       *kafka.Producer
	Consumer       *kafka.Consumer
	Config         karacakafka.KaracaKafkaConfig
	MessageHandler karacakafka.MessageHandler
	IsClosed       bool
	Logger         *log.Logger
}

type KaracaConsumer interface {
	StartConsume(messageHandler karacakafka.MessageHandler) error
	createTopicIfNotExist(topic string, numPartitions int, replicationFactor int, retention int) error
	createRetryTopic(topic string) error
	createErrorTopic(topic string) error
	createDeadTopic(topic string) error
	ensureTopicsExist() error
	subscribeToTopics() error
	messageHandler(message *kafka.Message)
	close()
	markAsClosed()
	publishMessageToRetryTopic(ctx context.Context, message kafka.Message, r any, deadTopicName string)
}

func NewKafkaConsumer(ctx context.Context, config karacakafka.KaracaKafkaConfig) KaracaConsumer {

	var (
		consumer *kafka.Consumer
		err      error
	)

	kafkaProducer, err := NewProducer(config.ProducerConfig)
	if err != nil {
		log.Printf("Error occurred when creating producer: %v", err)
	}

	kafkaAdmin, err := NewAdminFromProducer(kafkaProducer)
	if err != nil {
		log.Printf("Error occurred when creating admin client: %v", err)
	}

	readerConfig := &kafka.ConfigMap{
		"bootstrap.servers":        strings.Join(config.ReaderConfig.Brokers, ","),
		"group.id":                 config.ReaderConfig.GroupID,
		"auto.offset.reset":        config.ReaderConfig.AutoOffsetResetType,
		"allow.auto.create.topics": fmt.Sprintf("%t", config.ReaderConfig.AllowAutoCreateTopics),
		"enable.auto.commit":       fmt.Sprintf("%t", config.ReaderConfig.EnableAutoCommit),
		"fetch.max.bytes":          fmt.Sprintf("%d", config.ReaderConfig.FetchMaxBytes),
		"session.timeout.ms":       fmt.Sprintf("%d", config.ReaderConfig.SessionTimeout.Milliseconds()),
		"debug":                    config.ReaderConfig.Debug,
		"client.id":                config.ReaderConfig.ClientID,
	}

	consumer, err = kafka.NewConsumer(readerConfig)
	if err != nil {
		log.Printf("Error occurred when creating consumer: %v", err)
		time.Sleep(5 * time.Second)
	}

	return &karacaConsumer{
		Context:     ctx,
		AdminClient: kafkaAdmin.GetKafkaAdmin(),
		Producer:    kafkaProducer.GetKafkaProducer(),
		Consumer:    consumer,
		Config:      config,
		IsClosed:    false,
		Logger:      log.New(os.Stdout, "kafka-consumer: ", log.LstdFlags),
	}
}

func (c *karacaConsumer) StartConsume(MessageHandler karacakafka.MessageHandler) error {

	go c.close()

	c.MessageHandler = MessageHandler

	err := c.ensureTopicsExist()
	if err != nil {
		return err
	}

	err = c.subscribeToTopics()
	if err != nil {
		return err
	}

	var (
		message *kafka.Message
	)

	for {
		if c.IsClosed {
			c.Logger.Println("Marked IsClosed true so consumer closing and returning...")
			_ = c.Consumer.Close()
			return nil
		}

		message, err = c.Consumer.ReadMessage(time.Second * 5)
		if err != nil {
			if kafkaErr, ok := err.(kafka.Error); ok {
				if kafkaErr.Code() == kafka.ErrTimedOut {
					continue
				}
			}

			c.Logger.Printf("Error occurred when fetching message: %v", err)
			time.Sleep(5 * time.Second)
			continue
		} else {
			c.messageHandler(message)
		}
	}
}

func (c *karacaConsumer) createTopicIfNotExist(topic string, numPartitions int, replicationFactor int, retention int) error {
	retentionValue := fmt.Sprintf("%d", retention)

	_, err := c.AdminClient.CreateTopics(c.Context, []kafka.TopicSpecification{{
		Topic:             topic,
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
		Config: map[string]string{
			"retention.ms":        retentionValue,
			"min.insync.replicas": constants2.MinInSyncReplicas,
		},
	}})

	return err
}

func (c *karacaConsumer) createRetryTopic(topic string) error {
	return c.createTopicIfNotExist(topic, constants2.RetryTopicPartitionCount, constants2.RetryTopicReplicationFactor, constants2.RetryTopicRetention)
}

func (c *karacaConsumer) createErrorTopic(topic string) error {
	return c.createTopicIfNotExist(topic, constants2.ErrorTopicPartitionCount, constants2.ErrorTopicReplicationFactor, constants2.ErrorTopicRetention)
}

func (c *karacaConsumer) createDeadTopic(topic string) error {
	return c.createTopicIfNotExist(topic, constants2.DeadTopicPartitionCount, constants2.DeadTopicReplicationFactor, constants2.DeadTopicRetention)
}

func (c *karacaConsumer) ensureTopicsExist() error {
	var err error

	mainTopicName := c.Config.ConsumerConfig.Topics[0]

	retryTopicName := c.generateRetryTopicName(mainTopicName)
	errorTopicName := c.generateErrorTopicName(mainTopicName)
	deadTopicName := c.generateDeadTopicName(mainTopicName)

	err = c.createRetryTopic(retryTopicName)
	if err != nil {
		c.Logger.Printf("Error occurred when creating retry topic: %v", err)
		return err
	}

	err = c.createErrorTopic(errorTopicName)
	if err != nil {
		c.Logger.Printf("Error occurred when creating error topic: %v", err)
		return err
	}

	err = c.createDeadTopic(deadTopicName)
	if err != nil {
		c.Logger.Printf("Error occurred when creating dead topic: %v", err)
		return err
	}

	return nil
}

func (c *karacaConsumer) subscribeToTopics() error {
	var err error
	var topicsToSubscribe []string

	retryTopicName := c.Config.ConsumerConfig.AppName + constants.RetrySuffix

	topicsToSubscribe = append(topicsToSubscribe, c.Config.ConsumerConfig.Topics...)
	topicsToSubscribe = append(topicsToSubscribe, retryTopicName)

	err = c.Consumer.SubscribeTopics(topicsToSubscribe, nil)
	if err != nil {
		c.Logger.Printf("Error occurred when subscribing to topics: %v", err)
		return err
	}

	return nil
}

func (c *karacaConsumer) messageHandler(message *kafka.Message) {
	var err error

	timeStamp, _ := time.Parse("01/02/2006 15:04:05", TimeStamp(message.Headers))

	defer func() {
		if r := recover(); r != nil {

			IncrementRetryCount(message)

			retryTopicName := c.Config.ConsumerConfig.AppName + constants.RetrySuffix
			c.publishMessageToRetryTopic(c.Context, *message, r, retryTopicName)
		}
	}()

	err = c.MessageHandler(karacakafka.KafkaMessage{
		CorrelationId: CorrelationId(message.Headers),
		Timestamp:     timeStamp,
		Payload:       message.Value,
		Topic:         *message.TopicPartition.Topic,
		Partition:     int(message.TopicPartition.Partition),
		//todo: düzelt Headers:       //MapHeaders(message.Headers),
	})

	if err != nil {
		panic(err)
	}

	for _, err = c.Consumer.CommitMessage(message); err != nil; {
		c.Logger.Printf("Error occurred when committing message: %v", err)
		time.Sleep(5 * time.Second)
	}
}

func (c *karacaConsumer) close() {
	channel := make(chan os.Signal)
	signal.Notify(channel, syscall.SIGTERM, syscall.SIGINT)

	c.Logger.Println("Started to listen termination signal...")
	_ = <-channel
	c.Logger.Println("Handled termination signal...")
	c.markAsClosed()
}

func (c *karacaConsumer) markAsClosed() {
	c.IsClosed = true
	c.Logger.Printf("%s consumer marked as closed", c.Config.ConsumerConfig.AppName)
}

func (c *karacaConsumer) publishMessageToRetryTopic(
	ctx context.Context,
	message kafka.Message,
	r any,
	retryTopicName string) {

	var (
		ok           bool
		err          error
		kafkaMessage *kafka.Message
	)
	deliveryChan := make(chan kafka.Event)
	err, ok = r.(error)

	if !ok {
		err = fmt.Errorf("%v", r)
	}

	stack := make([]byte, 4<<10)
	length := runtime.Stack(stack, false)
	retryCount := RetryCount(message.Headers)

	retryMessage := prepareRetryMessage(message, retryCount, err,
		fmt.Sprintf("[Exception Recover] %v %s\n", err, stack[:length]), retryTopicName)

	for kafkaMessage == nil || kafkaMessage.TopicPartition.Error != nil {
		if kafkaMessage != nil && kafkaMessage.TopicPartition.Error != nil {
			c.Logger.Printf("Error occurred when producing dead message: %v", kafkaMessage.TopicPartition.Error)
			time.Sleep(5 * time.Second)
		}

		for err = c.Producer.Produce(&retryMessage, deliveryChan); err != nil; {
			c.Logger.Printf("Error occurred when producing dead message: %v", err)
			time.Sleep(5 * time.Second)
		}

		c.Logger.Printf("producer.Produce executed for CorrelationId: %s", CorrelationId(message.Headers))

		e := <-deliveryChan
		kafkaMessage = e.(*kafka.Message)

		c.Logger.Printf("deliveryChan executed for CorrelationId: %s", CorrelationId(message.Headers))
	}

	close(deliveryChan)

	for _, err = c.Consumer.CommitMessage(&message); err != nil; {
		c.Logger.Printf("Error occurred when committing message: %v", err)
		time.Sleep(5 * time.Second)
	}

	c.Logger.Printf("CommitMessage executed for CorrelationId: %s", CorrelationId(message.Headers))
}

// Todo: buradaki yardımcı metodları bir yerlere taşıyabilirsin gibi duruyor.
func removeMainSuffix(topic string) string {
	return strings.TrimSuffix(topic, constants.MainSuffix)
}

func (c *karacaConsumer) generateRetryTopicName(mainTopic string) string {
	cleanTopicName := removeMainSuffix(mainTopic)
	return fmt.Sprintf("%s_%s_retry", cleanTopicName, c.Config.ConsumerConfig.AppName)
}

func (c *karacaConsumer) generateErrorTopicName(mainTopic string) string {
	cleanTopicName := removeMainSuffix(mainTopic)
	return fmt.Sprintf("%s_%s_error", cleanTopicName, c.Config.ConsumerConfig.AppName)
}

func (c *karacaConsumer) generateDeadTopicName(mainTopic string) string {
	cleanTopicName := removeMainSuffix(mainTopic)
	return fmt.Sprintf("%s_%s_dead", cleanTopicName, c.Config.ConsumerConfig.AppName)
}

// todo: buradaki metodu doğru yerine gönder.
func prepareRetryMessage(message kafka.Message, retryCount int, err error, stackTracing string, topicName string) kafka.Message {
	var headers []kafka.Header

	timeNow := time.Now().UTC()
	timeStamp := timeNow.Format("01/02/2006 15:04:05")

	headers = append(headers, kafka.Header{
		Key:   "timeStamp",
		Value: []byte(timeStamp),
	})

	headers = append(headers, kafka.Header{
		Key:   "identityName",
		Value: []byte(IdentityName(message.Headers)),
	})

	headers = append(headers, kafka.Header{
		Key:   "identityType",
		Value: []byte(IdentityType(message.Headers)),
	})

	headers = append(headers, kafka.Header{
		Key:   "correlationId",
		Value: []byte(CorrelationId(message.Headers)),
	})

	headers = append(headers, kafka.Header{
		Key:   "userName",
		Value: []byte(UserName(message.Headers)),
	})

	headers = append(headers, kafka.Header{
		Key:   "type",
		Value: []byte(EventType(message.Headers)),
	})

	headers = append(headers, kafka.Header{
		Key:   "version",
		Value: []byte(Version(message.Headers)),
	})

	headers = append(headers, kafka.Header{
		Key:   "tenant",
		Value: []byte(Tenant(message.Headers)),
	})

	headers = append(headers, kafka.Header{
		Key:   "hash",
		Value: []byte(Hash(message.Headers)),
	})

	headers = append(headers, kafka.Header{
		Key:   "retryCount",
		Value: []byte(strconv.Itoa(retryCount)),
	})

	headers = append(headers, kafka.Header{
		Key:   "error",
		Value: []byte(err.Error()),
	})

	headers = append(headers, kafka.Header{
		Key:   "stackTracing",
		Value: []byte(stackTracing),
	})

	return kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topicName, Partition: kafka.PartitionAny},
		Key:            message.Key,
		Timestamp:      timeNow,
		Headers:        headers,
		Value:          message.Value,
	}
}
