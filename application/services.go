package application

import (
	"errors"

	"github.com/AndrivA89/kafkadesk/domain"
	"github.com/AndrivA89/kafkadesk/infrastructure"
)

type ClusterService struct {
	storage infrastructure.Storage
	kafka   infrastructure.KafkaClient
}

func NewClusterService(storage infrastructure.Storage, kafka infrastructure.KafkaClient) *ClusterService {
	return &ClusterService{storage: storage, kafka: kafka}
}

func (s *ClusterService) AddCluster(cluster domain.Cluster) error {
	if !cluster.IsValid() {
		return errors.New("invalid cluster configuration")
	}
	if err := s.storage.SaveCluster(cluster); err != nil {
		return err
	}
	return nil
}

func (s *ClusterService) GetTopics(clusterName string) ([]string, error) {
	cluster, err := s.storage.GetCluster(clusterName)
	if err != nil {
		return nil, err
	}
	return s.kafka.ListTopics(cluster)
}
