package infrastructure

import (
	"encoding/json"
	"errors"
	"os"

	"github.com/AndrivA89/kafkadesk/domain"
)

// Storage хранит информацию о кластерах и клиентах Kafka
type Storage struct {
	file     string
	clusters map[string]domain.Cluster // Конфигурации кластеров
	clients  map[string]*KafkaClient   // Клиенты Kafka
}

// NewStorage создаёт новый объект Storage
func NewStorage(file string) *Storage {
	return &Storage{
		file:     file,
		clusters: make(map[string]domain.Cluster),
		clients:  make(map[string]*KafkaClient),
	}
}

// GetClusterClient возвращает клиента Kafka для указанного кластера
func (s *Storage) GetClusterClient(name string) (*KafkaClient, error) {
	// Проверяем, есть ли клиент в памяти
	if client, exists := s.clients[name]; exists {
		return client, nil
	}

	// Если клиента нет, загружаем конфигурацию кластера
	cluster, exists := s.clusters[name]
	if !exists {
		return nil, errors.New("cluster not found")
	}

	// Создаём нового клиента Kafka
	client, err := NewKafkaClient(cluster)
	if err != nil {
		return nil, err
	}

	// Сохраняем клиента в память
	s.clients[name] = client
	return client, nil
}

// SaveCluster сохраняет конфигурацию кластера
func (s *Storage) SaveCluster(cluster domain.Cluster) error {
	s.clusters[cluster.Name] = cluster
	return s.saveToFile()
}

// LoadClusters загружает все конфигурации кластеров
func (s *Storage) LoadClusters() ([]domain.Cluster, error) {
	data, err := os.ReadFile(s.file)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// Если файл отсутствует, возвращаем пустой список
			return []domain.Cluster{}, nil
		}
		return nil, err
	}

	var clusters []domain.Cluster
	err = json.Unmarshal(data, &clusters)
	if err != nil {
		return nil, err
	}

	// Обновляем внутреннюю карту кластеров
	s.clusters = make(map[string]domain.Cluster)
	for _, cluster := range clusters {
		s.clusters[cluster.Name] = cluster
	}

	return clusters, nil
}

// GetCluster возвращает конфигурацию кластера по имени
func (s *Storage) GetCluster(name string) (domain.Cluster, error) {
	cluster, exists := s.clusters[name]
	if !exists {
		return domain.Cluster{}, errors.New("cluster not found")
	}
	return cluster, nil
}

// saveToFile сохраняет данные в файл
func (s *Storage) saveToFile() error {
	var clusters []domain.Cluster
	for _, cluster := range s.clusters {
		clusters = append(clusters, cluster)
	}

	data, err := json.Marshal(clusters)
	if err != nil {
		return err
	}

	return os.WriteFile(s.file, data, 0644)
}
