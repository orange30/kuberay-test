package historyserver

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/ray-project/kuberay/historyserver/pkg/collector/types"
	"github.com/ray-project/kuberay/historyserver/pkg/eventserver"
	"github.com/ray-project/kuberay/historyserver/pkg/storage"
	"github.com/sirupsen/logrus"
)

type ServerHandler struct {
	maxClusters          int
	rootDir              string
	dashboardDir         string
	rayGrafanaHost       string
	rayGrafanaIframeHost string
	rayPrometheusHost    string
	listenAddr           string

	reader        storage.StorageReader
	clientManager *ClientManager
	eventHandler  *eventserver.EventHandler
	httpClient    *http.Client
}

func NewServerHandler(c *types.RayHistoryServerConfig, dashboardDir string, listenAddr string, reader storage.StorageReader, clientManager *ClientManager, eventHandler *eventserver.EventHandler) *ServerHandler {
	if listenAddr == "" {
		listenAddr = ":8080"
	}
	return &ServerHandler{
		reader:        reader,
		clientManager: clientManager,
		eventHandler:  eventHandler,

		rootDir:              c.RootDir,
		rayGrafanaHost:       c.RayGrafanaHost,
		rayGrafanaIframeHost: c.RayGrafanaIframeHost,
		rayPrometheusHost:    c.RayPrometheusHost,
		dashboardDir:         dashboardDir,
		listenAddr:           listenAddr,
		// TODO: make this configurable
		maxClusters: 100,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (s *ServerHandler) Run(stop chan struct{}) error {
	s.RegisterRouter()
	port := s.listenAddr
	server := &http.Server{
		Addr:         port,             // Listen address
		ReadTimeout:  5 * time.Second,  // Read timeout
		WriteTimeout: 35 * time.Second, // Write response timeout (must be >= httpClient.Timeout for proxy requests)
		IdleTimeout:  60 * time.Second, // Idle timeout
	}
	go func() {
		logrus.Infof("Starting server on %s", port)
		err := server.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			logrus.Fatalf("Error starting server: %v", err)
		}
		logrus.Infof("Server stopped gracefully")
	}()

	<-stop
	logrus.Warnf("Receive stop single, so stop ray history server")
	// Create a context with 1 second timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Shutdown the server
	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Ray HistoryServer forced to shutdown: %v", err)
	}
	return nil
}
