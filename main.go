package kafkaclient

import (
	"log"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// onMessage receives topic, message
type onMessage func(string, string)

// KafkaClient ...
type KafkaClient struct {
	Server            string
	GroupID           string
	OnMessageReceived onMessage
}

// New creates broker object
func New(server string, groupID string, onMessageReceived onMessage) *KafkaClient {
	return &KafkaClient{
		Server:            server,
		GroupID:           groupID,
		OnMessageReceived: onMessageReceived,
	}
}

// ListenToTopics starts listening to specified topics
func (kb *KafkaClient) ListenToTopics(topics []string) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kb.Server,
		"group.id":          kb.GroupID,
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		log.Println(err)
		return
	}

	defer consumer.Close()
	err = consumer.SubscribeTopics(topics, nil)
	if err != nil {
		log.Println(err)
		return
	}

	log.Printf("Listening to kafka topics: %s", strings.Join(topics, ", "))

	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			if kb.OnMessageReceived != nil {
				kb.OnMessageReceived(*msg.TopicPartition.Topic, string(msg.Value))
			}
		} else {
			log.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
}

// ProduceToTopic Sends a message to the specified topic
func (kb *KafkaClient) ProduceToTopic(topic string, message string) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kb.Server})
	if err != nil {
		panic(err)
	}

	defer producer.Close()
	// Produce messages to topic (asynchronously)
	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(message),
	}, nil)

	if err != nil {
		log.Println(err)
	}

	// Wait for message deliveries before shutting down
	producer.Flush(15 * 1000)
}
