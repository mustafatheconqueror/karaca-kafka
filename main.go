package main

import "fmt"

func main() {
	fmt.Println("Hello World")
}

/*
brokers := []string{"localhost:9092"}
	producer := producer.NewKafkaProducer(brokers)

	err := producer.ProduceMessage("my-topic", []byte("key"), []byte("value"))
	if err != nil {
		log.Fatalf("Failed to produce message: %v", err)
	}

	producer.Close()*/
