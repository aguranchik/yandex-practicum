package main

import (
	"bytes"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAlertsHandler(t *testing.T) {
	var output bytes.Buffer
	handler := alertsHandler(log.New(&output, "", 0))
	request := httptest.NewRequest(http.MethodPost, "/alerts", strings.NewReader(`{
		"status":"firing",
		"alerts":[{
			"status":"firing",
			"labels":{"alertname":"KafkaBrokerDown","instance":"primary-kafka-3:9404","cluster":"primary"},
			"annotations":{"summary":"broker unavailable"}
		}]
	}`))
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusNoContent)
	}
	if !strings.Contains(output.String(), "alert=KafkaBrokerDown") {
		t.Fatalf("log output does not contain alert name: %q", output.String())
	}
}

func TestAlertsHandlerRejectsInvalidJSON(t *testing.T) {
	handler := alertsHandler(log.New(io.Discard, "", 0))
	request := httptest.NewRequest(http.MethodPost, "/alerts", strings.NewReader("not-json"))
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
}
