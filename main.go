package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/widget"
	"github.com/IBM/sarama"
	"github.com/xdg-go/scram"
)

type ClusterConfig struct {
	Name           string `json:"name"`
	Address        string `json:"address"`
	SASLUser       string `json:"sasl_user"`
	SASLPassword   string `json:"sasl_password"`
	SASLMechanism  string `json:"sasl_mechanism"`
	CACertPath     string `json:"ca_cert_path"`
	ClientCertPath string `json:"client_cert_path"`
	ClientKeyPath  string `json:"client_key_path"`
}

type AppState struct {
	clusters map[string]*KafkaClient
	config   []ClusterConfig
	active   string
}

type KafkaClient struct {
	client   sarama.Client
	producer sarama.SyncProducer
}

const configFileName = "clusters.json"

var topicList *widget.List

func main() {
	state := loadAppState()

	myApp := app.New()
	window := myApp.NewWindow("Kafka Desk")

	clusterList := widget.NewList(
		func() int { return len(state.config) },
		func() fyne.CanvasObject { return widget.NewLabel("Cluster") },
		func(id widget.ListItemID, obj fyne.CanvasObject) {
			obj.(*widget.Label).SetText(state.config[id].Name)
		},
	)

	clusterList.OnSelected = func(id widget.ListItemID) {
		if id >= 0 && id < len(state.config) {
			config := state.config[id]
			client, err := connectToCluster(config)
			if err != nil {
				dialog.ShowError(err, window)
				return
			}
			state.clusters[config.Name] = client
			state.active = config.Name
			topicList.Refresh()
		}
	}

	topicSearchEntry := widget.NewEntry()
	topicSearchEntry.SetPlaceHolder("Search topics...")
	topicList := widget.NewList(
		func() int {
			if state.active == "" {
				return 0
			}
			topics, _ := state.clusters[state.active].client.Topics()
			filtered := filterTopics(topics, topicSearchEntry.Text)
			return len(filtered)
		},
		func() fyne.CanvasObject { return widget.NewLabel("Topic") },
		func(id widget.ListItemID, obj fyne.CanvasObject) {
			if state.active == "" {
				return
			}
			topics, _ := state.clusters[state.active].client.Topics()
			filtered := filterTopics(topics, topicSearchEntry.Text)
			if id < len(filtered) {
				obj.(*widget.Label).SetText(filtered[id])
			}
		},
	)
	topicSearchEntry.OnChanged = func(_ string) {
		topicList.Refresh()
	}

	addClusterButton := widget.NewButton("Add Cluster", func() {
		openAddClusterDialog(state, window, clusterList)
	})

	leftMenu := container.NewVBox(
		widget.NewLabel("Clusters"),
		clusterList,
		addClusterButton,
	)

	mainContent := container.NewVBox(
		widget.NewLabel("Topics"),
		topicSearchEntry,
		topicList,
	)

	window.SetContent(container.NewBorder(nil, nil, leftMenu, nil, mainContent))
	window.Resize(fyne.NewSize(800, 600))
	window.ShowAndRun()
}

func loadAppState() *AppState {
	state := &AppState{
		clusters: make(map[string]*KafkaClient),
	}
	file, err := os.Open(configFileName)
	if err == nil {
		defer file.Close()
		json.NewDecoder(file).Decode(&state.config)
	}
	return state
}

func saveAppState(state *AppState) {
	file, err := os.Create(configFileName)
	if err == nil {
		defer file.Close()
		json.NewEncoder(file).Encode(state.config)
	}
}

func openAddClusterDialog(state *AppState, window fyne.Window, clusterList *widget.List) {
	nameEntry := widget.NewEntry()
	nameEntry.SetPlaceHolder("Cluster name")
	addressEntry := widget.NewEntry()
	addressEntry.SetPlaceHolder("Broker address")
	saslUserEntry := widget.NewEntry()
	saslUserEntry.SetPlaceHolder("SASL username")
	saslPasswordEntry := widget.NewPasswordEntry()
	saslPasswordEntry.SetPlaceHolder("SASL password")
	saslMechanismEntry := widget.NewSelect([]string{"SCRAM-SHA-256", "SCRAM-SHA-512"}, nil)
	caCertEntry := widget.NewEntry()
	caCertEntry.SetPlaceHolder("CA certificate path")
	clientCertEntry := widget.NewEntry()
	clientCertEntry.SetPlaceHolder("Client certificate path")
	clientKeyEntry := widget.NewEntry()
	clientKeyEntry.SetPlaceHolder("Client key path")
	dialog.ShowCustomConfirm("Add Cluster", "Add", "Cancel",
		container.NewVBox(
			nameEntry,
			addressEntry,
			saslUserEntry,
			saslPasswordEntry,
			saslMechanismEntry,
			caCertEntry,
			clientCertEntry,
			clientKeyEntry,
		), func(confirmed bool) {
			if confirmed {
				config := ClusterConfig{
					Name:           nameEntry.Text,
					Address:        addressEntry.Text,
					SASLUser:       saslUserEntry.Text,
					SASLPassword:   saslPasswordEntry.Text,
					SASLMechanism:  saslMechanismEntry.Selected,
					CACertPath:     caCertEntry.Text,
					ClientCertPath: clientCertEntry.Text,
					ClientKeyPath:  clientKeyEntry.Text,
				}
				state.config = append(state.config, config)
				saveAppState(state)
				clusterList.Refresh()
			}
		}, window)
}

func connectToCluster(config ClusterConfig) (*KafkaClient, error) {
	brokers := []string{config.Address}
	kafkaConfig := sarama.NewConfig()
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
	client, err := sarama.NewClient(brokers, kafkaConfig)
	if err != nil {
		return nil, err
	}
	return &KafkaClient{client: client}, nil
}

func filterTopics(topics []string, query string) []string {
	if query == "" {
		return topics
	}
	filtered := []string{}
	for _, topic := range topics {
		if strings.Contains(topic, query) {
			filtered = append(filtered, topic)
		}
	}
	return filtered
}

// ScramClient - структура клиента SCRAM
type ScramClient struct {
	conv   *scram.ClientConversation
	client scram.Client
}

// Begin инициализирует SCRAM аутентификацию
func (c *ScramClient) Begin(userName, password, authzID string) error {
	client, err := scram.SHA512.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	c.client = *client
	c.conv = c.client.NewConversation()
	return nil
}

// Step выполняет шаг SCRAM
func (c *ScramClient) Step(challenge string) (string, error) {
	return c.conv.Step(challenge)
}

// Done проверяет завершение SCRAM
func (c *ScramClient) Done() bool { return c.conv.Done() }

// SCRAMClientGenerator создаёт клиента SCRAM
func SCRAMClientGenerator(hashGenerator scram.HashGeneratorFcn, username, password string) func() sarama.SCRAMClient {
	return func() sarama.SCRAMClient {
		client, err := hashGenerator.NewClient(username, password, "")
		if err != nil {
			return nil
		}
		return &ScramClient{client: *client}
	}
}

func ConfigureSSL(config *sarama.Config, caCertPath, clientCertPath, clientKeyPath string) error {
	caCert, err := os.ReadFile(caCertPath)
	if err != nil {
		return err
	}
	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(caCert) {
		return fmt.Errorf("failed to append CA certificate")
	}
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = &tls.Config{
		RootCAs: certPool,
	}
	if clientCertPath != "" && clientKeyPath != "" {
		cert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
		if err != nil {
			return err
		}
		config.Net.TLS.Config.Certificates = []tls.Certificate{cert}
	}
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
