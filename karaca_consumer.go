package karaca_kafka

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type MessageHandler func(message KaracaMessage) error

type KaracaConsumer interface {
	StartConsume(messageHandler MessageHandler) error
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

type karacaConsumer struct {
	context.Context
	AdminClient    *kafka.AdminClient
	Producer       *kafka.Producer
	Consumer       *kafka.Consumer
	Config         KaracaKafkaConfig
	MessageHandler MessageHandler
	IsClosed       bool
}

func (kc *karacaConsumer) StartConsume(MessageHandler MessageHandler) error {

	go kc.close()

	kc.MessageHandler = MessageHandler

	err := kc.ensureTopicsExist()
	if err != nil {
		return err
	}

	err = kc.subscribeToTopics()
	if err != nil {
		return err
	}

	var (
		message *kafka.Message
	)

	for {
		if kc.IsClosed {
			log.Println("Marked IsClosed true so consumer closing and returning...")
			_ = kc.Consumer.Close()
			return nil
		}

		message, err = kc.Consumer.ReadMessage(time.Second * 5)
		if err != nil {
			if kafkaErr, ok := err.(kafka.Error); ok {
				if kafkaErr.Code() == kafka.ErrTimedOut {
					continue
				}
			}

			log.Printf("Error occurred when fetching message: %v", err)
			time.Sleep(5 * time.Second)
			continue
		} else {
			kc.messageHandler(message)
		}
	}
}

func (kc *karacaConsumer) createTopicIfNotExist(topic string, numPartitions int, replicationFactor int, retention int) error {

	retentionValue := fmt.Sprintf("%d", retention)

	_, err := kc.AdminClient.CreateTopics(kc.Context, []kafka.TopicSpecification{{
		Topic:             topic,
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
		Config: map[string]string{
			"retention.ms":        retentionValue,
			"min.insync.replicas": MinInSyncReplicas,
		},
	}})

	return err
}

func (kc *karacaConsumer) createRetryTopic(topic string) error {
	return kc.createTopicIfNotExist(topic, RetryTopicPartitionCount, RetryTopicReplicationFactor, RetryTopicRetention)
}

func (kc *karacaConsumer) createErrorTopic(topic string) error {
	return kc.createTopicIfNotExist(topic, ErrorTopicPartitionCount, ErrorTopicReplicationFactor, ErrorTopicRetention)
}

func (kc *karacaConsumer) createDeadTopic(topic string) error {
	return kc.createTopicIfNotExist(topic, DeadTopicPartitionCount, DeadTopicReplicationFactor, DeadTopicRetention)
}

func (kc *karacaConsumer) ensureTopicsExist() error {
	var err error

	mainTopicName := kc.Config.ConsumerConfig.Topics[0]

	retryTopicName := kc.generateRetryTopicName(mainTopicName)
	errorTopicName := kc.generateErrorTopicName(mainTopicName)
	deadTopicName := kc.generateDeadTopicName(mainTopicName)

	err = kc.createRetryTopic(retryTopicName)
	if err != nil {
		log.Printf("Error occurred when creating retry topic: %v", err)
		return err
	}

	err = kc.createErrorTopic(errorTopicName)
	if err != nil {
		log.Printf("Error occurred when creating error topic: %v", err)
		return err
	}

	err = kc.createDeadTopic(deadTopicName)
	if err != nil {
		log.Printf("Error occurred when creating dead topic: %v", err)
		return err
	}

	return nil
}

func (kc *karacaConsumer) subscribeToTopics() error {
	var err error
	var topicsToSubscribe []string

	retryTopicName := kc.Config.ConsumerConfig.AppName + RetrySuffix

	topicsToSubscribe = append(topicsToSubscribe, kc.Config.ConsumerConfig.Topics...)
	topicsToSubscribe = append(topicsToSubscribe, retryTopicName)

	err = kc.Consumer.SubscribeTopics(topicsToSubscribe, nil)
	if err != nil {
		log.Printf("Error occurred when subscribing to topics: %v", err)
		return err
	}

	return nil
}

func (kc *karacaConsumer) messageHandler(message *kafka.Message) {

	var (
		err error
	)

	timeStamp, _ := time.Parse("01/02/2006 15:04:05", TimeStamp(message.Headers))

	defer func() {
		if r := recover(); r != nil {

			IncrementRetryCount(message)

			retryTopicName := kc.Config.ConsumerConfig.AppName + RetrySuffix
			kc.publishMessageToRetryTopic(kc.Context, *message, r, retryTopicName)
		}
	}()

	err = kc.MessageHandler(KaracaMessage{
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

	for _, err = kc.Consumer.CommitMessage(message); err != nil; {
		log.Printf("Error occurred when committing message: %v", err)
		time.Sleep(5 * time.Second)
	}
}

func (kc *karacaConsumer) close() {
	channel := make(chan os.Signal)
	signal.Notify(channel, syscall.SIGTERM, syscall.SIGINT)

	log.Println("Started to listen termination signal...")
	_ = <-channel
	log.Println("Handled termination signal...")
	kc.markAsClosed()
}

func (kc *karacaConsumer) markAsClosed() {
	kc.IsClosed = true
	log.Printf("%s consumer marked as closed", kc.Config.ConsumerConfig.AppName)
}

func (kc *karacaConsumer) publishMessageToRetryTopic(
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
			log.Printf("Error occurred when producing dead message: %v", kafkaMessage.TopicPartition.Error)
			time.Sleep(5 * time.Second)
		}

		for err = kc.Producer.Produce(&retryMessage, deliveryChan); err != nil; {
			log.Printf("Error occurred when producing dead message: %v", err)
			time.Sleep(5 * time.Second)
		}

		log.Printf("producer.Produce executed for CorrelationId: %s", CorrelationId(message.Headers))

		e := <-deliveryChan
		kafkaMessage = e.(*kafka.Message)

		log.Printf("deliveryChan executed for CorrelationId: %s", CorrelationId(message.Headers))
	}

	close(deliveryChan)

	for _, err = kc.Consumer.CommitMessage(&message); err != nil; {
		log.Printf("Error occurred when committing message: %v", err)
		time.Sleep(5 * time.Second)
	}

	log.Printf("CommitMessage executed for CorrelationId: %s", CorrelationId(message.Headers))
}

// Todo: buradaki yardımcı metodları bir yerlere taşıyabilirsin gibi duruyor.
func removeMainSuffix(topic string) string {
	return strings.TrimSuffix(topic, MainSuffix)
}

func (kc *karacaConsumer) generateRetryTopicName(mainTopic string) string {
	cleanTopicName := removeMainSuffix(mainTopic)
	return fmt.Sprintf("%s_%s_retry", cleanTopicName, kc.Config.ConsumerConfig.AppName)
}

func (kc *karacaConsumer) generateErrorTopicName(mainTopic string) string {
	cleanTopicName := removeMainSuffix(mainTopic)
	return fmt.Sprintf("%s_%s_error", cleanTopicName, kc.Config.ConsumerConfig.AppName)
}

func (kc *karacaConsumer) generateDeadTopicName(mainTopic string) string {
	cleanTopicName := removeMainSuffix(mainTopic)
	return fmt.Sprintf("%s_%s_dead", cleanTopicName, kc.Config.ConsumerConfig.AppName)
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
