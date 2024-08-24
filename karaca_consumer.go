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
	PublishMessageToTopic(message kafka.Message, r any, topicName string, isErrorTopic bool)
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

	topicPrefix := kc.Config.ConsumerConfig.TopicDomainName + "." + kc.Config.ConsumerConfig.TopicSubDomainName

	retryTopicName := kc.generateRetryTopicName(topicPrefix)
	errorTopicName := kc.generateErrorTopicName(topicPrefix)
	deadTopicName := kc.generateDeadTopicName(topicPrefix)

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

	retryTopicName := kc.Config.ConsumerConfig.TopicDomainName + "." + kc.Config.ConsumerConfig.TopicSubDomainName + "_" + kc.Config.ConsumerConfig.AppName + RetrySuffix

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
			// Retry veya Dead Topic durumu belirlenir
			if shouldRetry := HandleRetryOrDeadMessage(message); shouldRetry {
				// Error Topic'e gönder
				errorTopicName := kc.Config.ConsumerConfig.TopicDomainName + "." + kc.Config.ConsumerConfig.TopicSubDomainName + "_" + kc.Config.ConsumerConfig.AppName + ErrorSuffix
				kc.PublishMessageToTopic(*message, r, errorTopicName, true)
			} else {
				// Dead Topic'e gönder
				deadTopicName := kc.Config.ConsumerConfig.TopicDomainName + "." + kc.Config.ConsumerConfig.TopicSubDomainName + "_" + kc.Config.ConsumerConfig.AppName + DeadSuffix
				kc.PublishMessageToTopic(*message, r, deadTopicName, false)
			}
		}
	}()

	// KaracaMessage oluşturma ve işleme
	karacaMsg := KaracaMessage{
		CorrelationId: CorrelationId(message.Headers),
		Timestamp:     timeStamp,
		Payload:       message.Value,
		Topic:         *message.TopicPartition.Topic,
		Partition:     int(message.TopicPartition.Partition),
		Headers:       mapHeaders(message.Headers),
	}

	// Handler fonksiyonunu çağırın
	err = kc.MessageHandler(karacaMsg)

	if err != nil {
		// KaracaMessage'dan güncellenmiş header'ları Kafka Message'a geri yazma
		//todo: refactor here
		for _, header := range message.Headers {
			if header.Key == "isRetryable" {
				header.Value = []byte(karacaMsg.Headers.IsRetryable)
			}
		}
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

func (kc *karacaConsumer) PublishMessageToTopic(message kafka.Message, r any, topicName string, isErrorTopic bool) {
	var (
		err          error
		kafkaMessage *kafka.Message
	)

	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)

	err, ok := r.(error)
	if !ok {
		err = fmt.Errorf("%v", r)
	}

	stack := make([]byte, 4<<10)
	length := runtime.Stack(stack, false)
	retryCount := RetryCount(message.Headers)

	var kafkaMsg kafka.Message
	if isErrorTopic {
		stackTrace := fmt.Sprintf("[Exception Recover] %v %s\n", err, stack[:length])
		kafkaMsg = prepareKafkaMessage(message, retryCount, err, stackTrace, topicName, true)
	} else {
		kafkaMsg = prepareKafkaMessage(message, retryCount, nil, "", topicName, false)
	}

	for kafkaMessage == nil || kafkaMessage.TopicPartition.Error != nil {
		if kafkaMessage != nil && kafkaMessage.TopicPartition.Error != nil {
			log.Printf("Error occurred when producing message to topic '%s': %v", topicName, kafkaMessage.TopicPartition.Error)
			time.Sleep(5 * time.Second)
		}

		for err = kc.Producer.Produce(&kafkaMsg, deliveryChan); err != nil; {
			log.Printf("Error occurred when producing message to topic '%s': %v", topicName, err)
			time.Sleep(5 * time.Second)
		}

		log.Printf("producer.Produce executed for CorrelationId: %s to topic '%s'", CorrelationId(message.Headers), topicName)

		e := <-deliveryChan
		kafkaMessage = e.(*kafka.Message)

		log.Printf("deliveryChan executed for CorrelationId: %s to topic '%s'", CorrelationId(message.Headers), topicName)
	}

	for _, err = kc.Consumer.CommitMessage(&message); err != nil; {
		log.Printf("Error occurred when committing message: %v", err)
		time.Sleep(5 * time.Second)
	}

	log.Printf("CommitMessage executed for CorrelationId: %s", CorrelationId(message.Headers))
}

func HandleRetryOrDeadMessage(message *kafka.Message) bool {

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
		return true // Retryable olarak işaretlenmiş, retry yapılacak
	}

	isRetryable := false
	retryCountIndex := -1

	// Header'ları tarayarak isRetryable ve retryCount değerlerini kontrol et
	for i, header := range message.Headers {
		switch header.Key {
		case isRetryableKey:
			if string(header.Value) == "true" {
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
		return true // Retryable olarak işaretlenmiş, retry yapılacak
	}

	// Eğer isRetryable değilse, direk dead topic'e gidecek
	return false
}

func (kc *karacaConsumer) generateRetryTopicName(topicPrefix string) string {

	return fmt.Sprintf("%s_%s_retry", topicPrefix, kc.Config.ConsumerConfig.AppName)
}

func (kc *karacaConsumer) generateErrorTopicName(topicPrefix string) string {
	return fmt.Sprintf("%s_%s_error", topicPrefix, kc.Config.ConsumerConfig.AppName)
}

func (kc *karacaConsumer) generateDeadTopicName(topicPrefix string) string {
	return fmt.Sprintf("%s_%s_dead", topicPrefix, kc.Config.ConsumerConfig.AppName)
}

func prepareKafkaMessage(
	message kafka.Message,
	retryCount int,
	err error,
	stackTracing string,
	topicName string,
	isRetryable bool,
) kafka.Message {
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

	if err != nil {
		headers = append(headers, kafka.Header{
			Key:   "error",
			Value: []byte(err.Error()),
		})
	}

	if stackTracing != "" {
		headers = append(headers, kafka.Header{
			Key:   "stackTracing",
			Value: []byte(stackTracing),
		})
	}

	headers = append(headers, kafka.Header{
		Key:   "isRetryable",
		Value: []byte(strconv.FormatBool(isRetryable)),
	})

	return kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topicName, Partition: kafka.PartitionAny},
		Key:            message.Key,
		Timestamp:      timeNow,
		Headers:        headers,
		Value:          message.Value,
	}
}

func NewKafkaConsumer(ctx context.Context, config KaracaKafkaConfig) KaracaConsumer {

	var (
		consumer *kafka.Consumer
		err      error
	)

	kafkaProducer, err := NewKaracaProducer(config.ProducerConfig)
	if err != nil {
		log.Printf("Error occurred when creating producer: %v", err)
	}

	kafkaAdmin, err := NewAdminFromKaracaProducer(kafkaProducer)
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
	}
}
