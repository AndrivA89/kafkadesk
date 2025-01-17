package infrastructure

import (
	"github.com/AndrivA89/kafkadesk/domain"
	"github.com/IBM/sarama"
)

type KafkaClient struct {
	client sarama.Client
}

// NewKafkaClient создает новый клиент Kafka
func NewKafkaClient(config domain.Cluster) (*KafkaClient, error) {
	kafkaConfig := sarama.NewConfig()
	// Настройка SASL/SSL
	if config.SASLUser != "" {
		err := ConfigureSASL(kafkaConfig, config.SASLUser, config.SASLPassword, config.SASLMechanism)
		if err != nil {
			return nil, err
		}
	}
	if config.CACertPath != "" {
		err := ConfigureSSL(kafkaConfig, config.CACertPath, config.ClientCertPath, config.ClientKeyPath)
		if err != nil {
			return nil, err
		}
	}
	client, err := sarama.NewClient([]string{config.Address}, kafkaConfig)
	if err != nil {
		return nil, err
	}
	return &KafkaClient{client: client}, nil
}

// ListTopics возвращает список топиков
func (k *KafkaClient) ListTopics() ([]string, error) {
	return k.client.Topics()
}

// ProduceMessage отправляет сообщение в топик
func (k *KafkaClient) ProduceMessage(topic string, message []byte) error {
	producer, err := sarama.NewSyncProducerFromClient(k.client)
	if err != nil {
		return err
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(message),
	}
	_, _, err = producer.SendMessage(msg)
	return err
}
