package infrastructure

import (
	"encoding/json"
	"errors"
	"os"

	"github.com/AndrivA89/kafkadesk/domain"
)

type Storage struct {
	file string
}

func NewStorage(file string) *Storage {
	return &Storage{file: file}
}

func (s *Storage) SaveCluster(cluster domain.Cluster) error {
	clusters, err := s.LoadClusters()
	if err != nil {
		clusters = []domain.Cluster{}
	}
	clusters = append(clusters, cluster)
	data, _ := json.Marshal(clusters)
	return os.WriteFile(s.file, data, 0644)
}

func (s *Storage) LoadClusters() ([]domain.Cluster, error) {
	data, err := os.ReadFile(s.file)
	if err != nil {
		return nil, err
	}
	var clusters []domain.Cluster
	err = json.Unmarshal(data, &clusters)
	return clusters, err
}

func (s *Storage) GetCluster(name string) (domain.Cluster, error) {
	clusters, err := s.LoadClusters()
	if err != nil {
		return domain.Cluster{}, err
	}
	for _, c := range clusters {
		if c.Name == name {
			return c, nil
		}
	}
	return domain.Cluster{}, errors.New("cluster not found")
}
