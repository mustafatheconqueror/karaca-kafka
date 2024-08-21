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
	"strings"
	"syscall"
	"time"
)

type handler func(message kafka_message.KafkaMessage) error

type Consumer interface {
	CreateTopics() []string
	EnsureTopicsExist() error
	SubscribeToTopics() error
	CreateMainTopic(topic string) error
	CreateRetryTopic(topic string) error
	CreateErrorTopic(topic string) error
	CreateDeadTopic(topic string) error
	StartConsume(handler handler) error
	handle(message *kafka.Message)
	Close()
	createTopicIfNotExist(topic string, numPartitions int, replicationFactor int, retention int) error
	MarkAsClosed()
	PublishMessageToRetryTopic(ctx context.Context, message kafka.Message, r any, deadTopicName string)
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

	err := c.EnsureTopicsExist()
	if err != nil {
		return err
	}

	err = c.SubscribeToTopics()
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
			c.handle(message)
		}
	}
}

func (c *kafkaConsumer) EnsureTopicsExist() error {
	var err error

	mainTopicName := c.Config.Topics[0]

	retryTopicName := c.generateRetryTopicName(mainTopicName)
	errorTopicName := c.generateErrorTopicName(mainTopicName)
	deadTopicName := c.generateDeadTopicName(mainTopicName)

	err = c.CreateRetryTopic(retryTopicName)
	if err != nil {
		c.Logger.Printf("Error occurred when creating retry topic: %v", err)
		return err
	}

	err = c.CreateErrorTopic(errorTopicName)
	if err != nil {
		c.Logger.Printf("Error occurred when creating error topic: %v", err)
		return err
	}

	err = c.CreateDeadTopic(deadTopicName)
	if err != nil {
		c.Logger.Printf("Error occurred when creating dead topic: %v", err)
		return err
	}

	return nil
}

func (c *kafkaConsumer) SubscribeToTopics() error {
	var err error
	var topicsToSubscribe []string

	retryTopicName := c.Config.AppName + constants.RetrySuffix

	topicsToSubscribe = append(topicsToSubscribe, c.Config.Topics...)
	topicsToSubscribe = append(topicsToSubscribe, retryTopicName)

	err = c.Consumer.SubscribeTopics(topicsToSubscribe, nil)
	if err != nil {
		c.Logger.Printf("Error occurred when subscribing to topics: %v", err)
		return err
	}

	return nil
}

func (c *kafkaConsumer) CreateTopics() []string {
	//Todo: Subscribe topics diye bir metoda böl, topicleri create ettiğini başka yerde yap.
	var topicsToSubscribe []string

	retryTopicName := c.Config.AppName + constants.RetrySuffix
	errorTopicName := c.Config.AppName + constants.ErrorSuffix
	deadTopicName := c.Config.AppName + constants.DeadSuffix

	topicsToSubscribe = append(topicsToSubscribe, c.Config.Topics...)
	topicsToSubscribe = append(topicsToSubscribe, retryTopicName)

	//todo: retry topici yoksa ne olacak :)
	//todo: buraya bir sonsuz döngü yapılmış burayı düzelt.
	for err := c.CreateRetryTopic(retryTopicName); err != nil; {
		c.Logger.Printf("Error occurred when creating retry topic: %v", err)
		time.Sleep(5 * time.Second)
	}

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

			retryTopicName := c.Config.AppName + constants.RetrySuffix
			c.PublishMessageToRetryTopic(c.Context, *message, r, retryTopicName)
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

func (c *kafkaConsumer) PublishMessageToRetryTopic(
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
	retryCount := kafka_message.RetryCount(message.Headers)

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
func removeMainSuffix(topic string) string {
	return strings.TrimSuffix(topic, constants.MainSuffix)
}

func (c *kafkaConsumer) generateRetryTopicName(mainTopic string) string {
	cleanTopicName := removeMainSuffix(mainTopic)
	return fmt.Sprintf("%s_%s_retry", cleanTopicName, c.Config.AppName)
}

func (c *kafkaConsumer) generateErrorTopicName(mainTopic string) string {
	cleanTopicName := removeMainSuffix(mainTopic)
	return fmt.Sprintf("%s_%s_error", cleanTopicName, c.Config.AppName)
}

func (c *kafkaConsumer) generateDeadTopicName(mainTopic string) string {
	cleanTopicName := removeMainSuffix(mainTopic)
	return fmt.Sprintf("%s_%s_dead", cleanTopicName, c.Config.AppName)
}

func NewKafkaConsumer(ctx context.Context, consumerConfig config.ConsumerConfig) Consumer {

	var (
		consumer *kafka.Consumer
		err      error
	)

	kafkaProducer, err := producer.NewProducer(consumerConfig.Brokers)
	if err != nil {
		log.Printf("Error occurred when creating producer: %v", err)
	}

	kafkaAdmin, err := kafka_admin.NewAdminFromProducer(kafkaProducer)
	if err != nil {
		log.Printf("Error occurred when creating admin client: %v", err)
	}

	readerConfig := config.NewKafkaReaderConfig(consumerConfig.Brokers, consumerConfig.AppName)
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
