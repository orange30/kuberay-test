package main

import (
	"encoding/json"
	"flag"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/ray-project/kuberay/historyserver/pkg/collector"
	"github.com/ray-project/kuberay/historyserver/pkg/collector/types"
	"github.com/ray-project/kuberay/historyserver/pkg/eventserver"
	"github.com/ray-project/kuberay/historyserver/pkg/historyserver"
	"github.com/sirupsen/logrus"
)

func main() {
	runtimeClassName := ""
	rayRootDir := ""
	kubeconfigs := ""
	runtimeClassConfigPath := "/var/collector-config/data"
	dashboardDir := ""
	port := 8080
	flag.StringVar(&runtimeClassName, "runtime-class-name", "", "")
	flag.StringVar(&rayRootDir, "ray-root-dir", "", "")
	flag.StringVar(&kubeconfigs, "kubeconfigs", "", "")
	flag.StringVar(&dashboardDir, "dashboard-dir", "/dashboard", "")
	flag.StringVar(&runtimeClassConfigPath, "runtime-class-config-path", "", "") //"/var/collector-config/data"
	flag.IntVar(&port, "port", 8080, "HTTP listen port")
	flag.Parse()

	cliMgr := historyserver.NewClientManager(kubeconfigs)

	jsonData := make(map[string]interface{})
	if runtimeClassConfigPath != "" {
		data, err := os.ReadFile(runtimeClassConfigPath)
		if err != nil {
			panic("Failed to read runtime class config " + err.Error())
		}
		err = json.Unmarshal(data, &jsonData)
		if err != nil {
			panic("Failed to parse runtime class config: " + err.Error())
		}
	}

	registry := collector.GetReaderRegistry()
	factory, ok := registry[runtimeClassName]
	if !ok {
		panic("Not supported runtime class name: " + runtimeClassName + ".")
	}

	globalConfig := types.RayHistoryServerConfig{
		RootDir:              rayRootDir,
		RayGrafanaHost:       os.Getenv("RAY_GRAFANA_HOST"),
		RayGrafanaIframeHost: os.Getenv("RAY_GRAFANA_IFRAME_HOST"),
		RayPrometheusHost:    os.Getenv("RAY_PROMETHEUS_HOST"),
	}

	reader, err := factory(&globalConfig, jsonData)
	if err != nil {
		panic("Failed to create reader for runtime class name: " + runtimeClassName + ".")
	}

	// Create EventHandler with storage reader
	eventHandler := eventserver.NewEventHandler(reader)

	// WaitGroup to track goroutine completion
	var wg sync.WaitGroup

	// Start EventHandler in background goroutine
	eventStop := make(chan struct{}, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		logrus.Info("Starting EventHandler in background...")
		if err := eventHandler.Run(eventStop, 2); err != nil {
			logrus.Errorf("EventHandler stopped with error: %v", err)
		}
		logrus.Info("EventHandler shutdown complete")
	}()

	listenAddr := ":" + strconv.Itoa(port)
	handler := historyserver.NewServerHandler(&globalConfig, dashboardDir, listenAddr, reader, cliMgr, eventHandler)

	sigChan := make(chan os.Signal, 1)
	stop := make(chan struct{}, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	wg.Add(1)
	go func() {
		defer wg.Done()
		handler.Run(stop)
		logrus.Info("HTTP server shutdown complete")
	}()

	<-sigChan
	logrus.Info("Received shutdown signal, initiating graceful shutdown...")

	// Stop both the server and the event handler
	stop <- struct{}{}
	eventStop <- struct{}{}

	// Wait for both goroutines to complete
	wg.Wait()
	logrus.Info("Graceful shutdown complete")
}
