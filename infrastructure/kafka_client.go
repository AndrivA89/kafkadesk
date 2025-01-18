package infrastructure

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/IBM/sarama"
	"github.com/xdg-go/scram"

	"github.com/AndrivA89/kafkadesk/domain"
)

type SCRAMClient struct {
	client           *scram.Client
	conversation     *scram.ClientConversation
	hashGeneratorFcn scram.HashGeneratorFcn
}

// Begin начинает SCRAM-аутентификацию
func (c *SCRAMClient) Begin(userName, password, authzID string) error {
	client, err := c.hashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	c.client = client
	c.conversation = client.NewConversation()
	return nil
}

// Step выполняет шаг SCRAM-аутентификации
func (c *SCRAMClient) Step(challenge string) (string, error) {
	return c.conversation.Step(challenge)
}

// Done проверяет завершение аутентификации
func (c *SCRAMClient) Done() bool {
	return c.conversation.Done()
}

type KafkaClient struct {
	client sarama.Client
}

func NewKafkaClient(config domain.Cluster) (*KafkaClient, error) {
	kafkaConfig := sarama.NewConfig()

	// Настраиваем SASL
	if config.SASLUser != "" {
		err := ConfigureSASL(kafkaConfig, config.SASLUser, config.SASLPassword, config.SASLMechanism)
		if err != nil {
			return nil, err
		}
	}

	// Настраиваем SSL
	if config.CACertPath != "" {
		err := ConfigureSSL(kafkaConfig, config.CACertPath, config.ClientCertPath, config.ClientKeyPath)
		if err != nil {
			return nil, err
		}
	}

	// Создаём клиент Kafka
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

func ConfigureSSL(config *sarama.Config, caCertPath, clientCertPath, clientKeyPath string) error {
	// Читаем CA сертификат
	caCert, err := os.ReadFile(caCertPath)
	if err != nil {
		return fmt.Errorf("failed to read CA certificate: %w", err)
	}

	// Добавляем сертификат в пул доверенных
	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(caCert) {
		return fmt.Errorf("failed to append CA certificate")
	}

	// Настраиваем конфигурацию TLS
	tlsConfig := &tls.Config{
		RootCAs: certPool,
	}

	// Читаем клиентский сертификат и ключ (если они указаны)
	if clientCertPath != "" && clientKeyPath != "" {
		clientCert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
		if err != nil {
			return fmt.Errorf("failed to load client certificate and key: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{clientCert}
	}

	// Включаем TLS в конфигурации Kafka
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = tlsConfig

	return nil
}

func ConfigureSASL(config *sarama.Config, username, password, mechanism string) error {
	config.Net.SASL.Enable = true
	config.Net.SASL.User = username
	config.Net.SASL.Password = password

	switch mechanism {
	case "SCRAM-SHA-256":
		config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		config.Net.SASL.SCRAMClientGeneratorFunc = SCRAMClientGenerator(scram.SHA256, username, password)
	case "SCRAM-SHA-512":
		config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		config.Net.SASL.SCRAMClientGeneratorFunc = SCRAMClientGenerator(scram.SHA512, username, password)
	default:
		return fmt.Errorf("unsupported SASL mechanism: %s", mechanism)
	}
	return nil
}

func SCRAMClientGenerator(hashGen scram.HashGeneratorFcn, username, password string) func() sarama.SCRAMClient {
	return func() sarama.SCRAMClient {
		client, err := hashGen.NewClient(username, password, "")
		if err != nil {
			panic(fmt.Errorf("failed to create SCRAM client: %v", err)) // Обрабатываем ошибку
		}
		return &SCRAMClient{client: client, hashGeneratorFcn: hashGen}
	}
}
