package main

import (
	"github.com/AndrivA89/kafkadesk/application"
	"github.com/AndrivA89/kafkadesk/domain"
	"github.com/AndrivA89/kafkadesk/infrastructure"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/widget"
)

func main() {
	storage := infrastructure.NewStorage("clusters.json")
	kafkaClient := infrastructure.KafkaClient{}
	clusterService := application.NewClusterService(storage, kafkaClient)

	myApp := app.New()
	window := myApp.NewWindow("Kafka Desk")

	clusterList := widget.NewList(
		func() int {
			clusters, _ := storage.LoadClusters()
			return len(clusters)
		},
		func() fyne.CanvasObject { return widget.NewLabel("Cluster") },
		func(id widget.ListItemID, obj fyne.CanvasObject) {
			clusters, _ := storage.LoadClusters()
			obj.(*widget.Label).SetText(clusters[id].Name)
		},
	)

	addClusterButton := widget.NewButton("Add Cluster", func() {
		nameEntry := widget.NewEntry()
		nameEntry.SetPlaceHolder("Cluster name")
		addressEntry := widget.NewEntry()
		addressEntry.SetPlaceHolder("Broker address")
		dialog.ShowCustomConfirm("Add Cluster", "Add", "Cancel",
			container.NewVBox(nameEntry, addressEntry), func(confirmed bool) {
				if confirmed {
					cluster := domain.Cluster{Name: nameEntry.Text, Address: addressEntry.Text}
					if err := clusterService.AddCluster(cluster); err != nil {
						dialog.ShowError(err, window)
					}
					clusterList.Refresh()
				}
			}, window)
	})

	leftMenu := container.NewVBox(
		clusterList,
		addClusterButton,
	)

	window.SetContent(container.NewBorder(nil, nil, leftMenu, nil, nil))
	window.Resize(fyne.NewSize(800, 600))
	window.ShowAndRun()
}
