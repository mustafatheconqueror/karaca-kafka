package consumer

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/mustafatheconqueror/karaca-kafka/config"
	"github.com/mustafatheconqueror/karaca-kafka/constants"
	"github.com/mustafatheconqueror/karaca-kafka/kafka_admin"
	"github.com/mustafatheconqueror/karaca-kafka/kafka_message"
	"github.com/mustafatheconqueror/karaca-kafka/producer"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
	"time"
)

type handler func(message kafka_message.KafkaMessage) error

type Consumer interface {
	CreateTopics() []string
	CreateMainTopic(topic string) error
	CreateRetryTopic(topic string) error
	CreateErrorTopic(topic string) error
	CreateDeadTopic(topic string) error
	StartConsume(handler handler) error
	handle(message *kafka.Message)
	Close()
	createTopicIfNotExist(topic string, numPartitions int, replicationFactor int, retention int) error
	MarkAsClosed()
	PublishMessageToDeadTopic(ctx context.Context, message kafka.Message, r any, deadTopicName string)
}

type kafkaConsumer struct {
	Context     context.Context
	AdminClient *kafka.AdminClient
	Producer    *kafka.Producer
	Consumer    *kafka.Consumer
	Config      config.ConsumerConfig
	Handler     handler
	IsClosed    bool
	Logger      *log.Logger
}

func (c *kafkaConsumer) StartConsume(handler handler) error {
	go c.Close()

	c.Handler = handler
	var err error

	topicsToSubscribe := c.CreateTopics()

	for err = c.Consumer.SubscribeTopics(topicsToSubscribe, nil); err != nil; {
		c.Logger.Printf("Error occurred when consumer subscribe topics: %v", err)
		time.Sleep(5 * time.Second)
	}

	var message *kafka.Message
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
			c.handle(message)
		}
	}
}

func (c *kafkaConsumer) CreateTopics() []string {

	var topicsToSubscribe []string

	mainTopicName := c.Config.AppName + constants.MainSuffix
	retryTopicName := c.Config.AppName + constants.RetrySuffix
	errorTopicName := c.Config.AppName + constants.ErrorSuffix
	deadTopicName := c.Config.AppName + constants.DeadSuffix

	for err := c.CreateMainTopic(mainTopicName); err != nil; {
		c.Logger.Printf("Error occurred when creating main topic: %v", err)
		time.Sleep(5 * time.Second)
	}
	topicsToSubscribe = append(topicsToSubscribe, mainTopicName)

	for err := c.CreateRetryTopic(retryTopicName); err != nil; {
		c.Logger.Printf("Error occurred when creating retry topic: %v", err)
		time.Sleep(5 * time.Second)
	}
	topicsToSubscribe = append(topicsToSubscribe, retryTopicName)

	for err := c.CreateErrorTopic(errorTopicName); err != nil; {
		c.Logger.Printf("Error occurred when creating error topic: %v", err)
		time.Sleep(5 * time.Second)
	}

	for err := c.CreateDeadTopic(deadTopicName); err != nil; {
		c.Logger.Printf("Error occurred when creating dead topic: %v", err)
		time.Sleep(5 * time.Second)
	}

	return topicsToSubscribe
}

func (c *kafkaConsumer) handle(message *kafka.Message) {
	var err error

	timeStamp, _ := time.Parse("01/02/2006 15:04:05", kafka_message.TimeStamp(message.Headers))

	defer func() {
		if r := recover(); r != nil {

			kafka_message.IncrementRetryCount(message)

			deadTopicName := c.Config.AppName + constants.DeadSuffix
			c.PublishMessageToDeadTopic(c.Context, *message, r, deadTopicName)
		}
	}()

	err = c.Handler(kafka_message.KafkaMessage{
		CorrelationId: kafka_message.CorrelationId(message.Headers),
		Timestamp:     timeStamp,
		Payload:       message.Value,
		Topic:         *message.TopicPartition.Topic,
		Partition:     int(message.TopicPartition.Partition),
		Headers:       kafka_message.MapHeaders(message.Headers),
	})

	if err != nil {
		panic(err)
	}

	for _, err = c.Consumer.CommitMessage(message); err != nil; {
		c.Logger.Printf("Error occurred when committing message: %v", err)
		time.Sleep(5 * time.Second)
	}
}

func (c *kafkaConsumer) Close() {
	channel := make(chan os.Signal)
	signal.Notify(channel, syscall.SIGTERM, syscall.SIGINT)

	c.Logger.Println("Started to listen termination signal...")
	_ = <-channel
	c.Logger.Println("Handled termination signal...")
	c.MarkAsClosed()
}

func (c *kafkaConsumer) CreateMainTopic(topic string) error {
	return c.createTopicIfNotExist(topic, constants.MainTopicPartitionCount, constants.MainTopicReplicationFactor, constants.MainTopicRetention)
}

func (c *kafkaConsumer) CreateRetryTopic(topic string) error {
	return c.createTopicIfNotExist(topic, constants.RetryTopicPartitionCount, constants.RetryTopicReplicationFactor, constants.RetryTopicRetention)
}

func (c *kafkaConsumer) CreateErrorTopic(topic string) error {
	return c.createTopicIfNotExist(topic, constants.ErrorTopicPartitionCount, constants.ErrorTopicReplicationFactor, constants.ErrorTopicRetention)
}

func (c *kafkaConsumer) CreateDeadTopic(topic string) error {
	return c.createTopicIfNotExist(topic, constants.DeadTopicPartitionCount, constants.DeadTopicReplicationFactor, constants.DeadTopicRetention)
}

func (c *kafkaConsumer) createTopicIfNotExist(topic string, numPartitions int, replicationFactor int, retention int) error {
	retentionValue := fmt.Sprintf("%d", retention)

	_, err := c.AdminClient.CreateTopics(c.Context, []kafka.TopicSpecification{{
		Topic:             topic,
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
		Config: map[string]string{
			"retention.ms":        retentionValue,
			"min.insync.replicas": constants.MinInSyncReplicas,
		},
	}})

	return err
}

func (c *kafkaConsumer) MarkAsClosed() {
	c.IsClosed = true
	c.Logger.Printf("%s consumer marked as closed", c.Config.AppName)
}

func (c *kafkaConsumer) PublishMessageToDeadTopic(
	ctx context.Context,
	message kafka.Message,
	r any,
	deadTopicName string) {

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
	retryCount := kafka_message.RetryCount(message.Headers)

	deadMessage := prepareDeadMessage(message, retryCount, err,
		fmt.Sprintf("[Exception Recover] %v %s\n", err, stack[:length]), deadTopicName)

	for kafkaMessage == nil || kafkaMessage.TopicPartition.Error != nil {
		if kafkaMessage != nil && kafkaMessage.TopicPartition.Error != nil {
			c.Logger.Printf("Error occurred when producing dead message: %v", kafkaMessage.TopicPartition.Error)
			time.Sleep(5 * time.Second)
		}

		for err = c.Producer.Produce(&deadMessage, deliveryChan); err != nil; {
			c.Logger.Printf("Error occurred when producing dead message: %v", err)
			time.Sleep(5 * time.Second)
		}

		c.Logger.Printf("producer.Produce executed for CorrelationId: %s", kafka_message.CorrelationId(message.Headers))

		e := <-deliveryChan
		kafkaMessage = e.(*kafka.Message)

		c.Logger.Printf("deliveryChan executed for CorrelationId: %s", kafka_message.CorrelationId(message.Headers))
	}

	close(deliveryChan)

	for _, err = c.Consumer.CommitMessage(&message); err != nil; {
		c.Logger.Printf("Error occurred when committing message: %v", err)
		time.Sleep(5 * time.Second)
	}

	c.Logger.Printf("CommitMessage executed for CorrelationId: %s", kafka_message.CorrelationId(message.Headers))
}

func prepareDeadMessage(message kafka.Message, retryCount int, err error, stackTracing string, topicName string) kafka.Message {
	var headers []kafka.Header

	timeNow := time.Now().UTC()
	timeStamp := timeNow.Format("01/02/2006 15:04:05")

	headers = append(headers, kafka.Header{
		Key:   "timeStamp",
		Value: []byte(timeStamp),
	})

	headers = append(headers, kafka.Header{
		Key:   "identityName",
		Value: []byte(kafka_message.IdentityName(message.Headers)),
	})

	headers = append(headers, kafka.Header{
		Key:   "identityType",
		Value: []byte(kafka_message.IdentityType(message.Headers)),
	})

	headers = append(headers, kafka.Header{
		Key:   "correlationId",
		Value: []byte(kafka_message.CorrelationId(message.Headers)),
	})

	headers = append(headers, kafka.Header{
		Key:   "userName",
		Value: []byte(kafka_message.UserName(message.Headers)),
	})

	headers = append(headers, kafka.Header{
		Key:   "type",
		Value: []byte(kafka_message.EventType(message.Headers)),
	})

	headers = append(headers, kafka.Header{
		Key:   "version",
		Value: []byte(kafka_message.Version(message.Headers)),
	})

	headers = append(headers, kafka.Header{
		Key:   "tenant",
		Value: []byte(kafka_message.Tenant(message.Headers)),
	})

	headers = append(headers, kafka.Header{
		Key:   "hash",
		Value: []byte(kafka_message.Hash(message.Headers)),
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

func NewKafkaConsumer(ctx context.Context, consumerConfig config.ConsumerConfig, brokers []string) Consumer {

	var (
		consumer *kafka.Consumer
		err      error
	)

	kafkaProducer, err := producer.NewProducer(brokers)
	if err != nil {
		log.Printf("Error occurred when creating producer: %v", err)
	}

	kafkaAdmin, err := kafka_admin.NewAdminFromProducer(kafkaProducer)
	if err != nil {
		log.Printf("Error occurred when creating admin client: %v", err)
	}

	readerConfig := config.NewKafkaReaderConfig(brokers, consumerConfig.AppName)
	readerConfigMap := readerConfig.ToKafkaConfigMap()

	consumer, err = kafka.NewConsumer(readerConfigMap)
	if err != nil {
		log.Printf("Error occurred when creating consumer: %v", err)
		time.Sleep(5 * time.Second)
	}

	return &kafkaConsumer{
		Context:     ctx,
		AdminClient: kafkaAdmin.GetKafkaAdmin(),
		Producer:    kafkaProducer.GetKafkaProducer(),
		Consumer:    consumer,
		Config:      consumerConfig,
		IsClosed:    false,
		Logger:      log.New(os.Stdout, "kafka-consumer: ", log.LstdFlags),
	}
}
