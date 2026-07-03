package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type webhookPayload struct {
	Status string `json:"status"`
	Alerts []struct {
		Status      string            `json:"status"`
		Labels      map[string]string `json:"labels"`
		Annotations map[string]string `json:"annotations"`
	} `json:"alerts"`
}

func alertsHandler(logger *log.Logger) http.HandlerFunc {
	return func(response http.ResponseWriter, request *http.Request) {
		defer request.Body.Close()
		var payload webhookPayload
		decoder := json.NewDecoder(http.MaxBytesReader(response, request.Body, 1<<20))
		if err := decoder.Decode(&payload); err != nil {
			http.Error(response, fmt.Sprintf("decode alert payload: %v", err), http.StatusBadRequest)
			return
		}

		for _, alert := range payload.Alerts {
			logger.Printf(
				"status=%s alert=%s instance=%s cluster=%s summary=%q",
				alert.Status,
				alert.Labels["alertname"],
				alert.Labels["instance"],
				alert.Labels["cluster"],
				alert.Annotations["summary"],
			)
		}
		response.WriteHeader(http.StatusNoContent)
	}
}

func main() {
	logger := log.New(os.Stdout, "alert-receiver ", log.Ldate|log.Ltime|log.LUTC)
	mux := http.NewServeMux()
	mux.HandleFunc("/alerts", alertsHandler(logger))
	mux.HandleFunc("/health", func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusNoContent)
	})

	server := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		logger.Printf("listening on %s", server.Addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal(err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	if err := server.Close(); err != nil {
		logger.Printf("close server: %v", err)
	}
}
