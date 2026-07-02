package registry

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"
)

type Client struct {
	baseURL  *url.URL
	username string
	password string
	http     *http.Client
}

type schemaResponse struct {
	ID         int    `json:"id"`
	Schema     string `json:"schema"`
	SchemaType string `json:"schemaType"`
}

func New(rawURL, username, password, caFile string) (*Client, error) {
	baseURL, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse Schema Registry URL: %w", err)
	}
	if baseURL.Scheme != "https" && baseURL.Scheme != "http" {
		return nil, fmt.Errorf("unsupported Schema Registry URL scheme %q", baseURL.Scheme)
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	if baseURL.Scheme == "https" {
		certificate, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read Schema Registry CA certificate %q: %w", caFile, err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(certificate) {
			return nil, fmt.Errorf("Schema Registry CA file %q is not valid PEM", caFile)
		}
		transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool}
	}

	return &Client{
		baseURL:  baseURL,
		username: username,
		password: password,
		http:     &http.Client{Transport: transport, Timeout: 15 * time.Second},
	}, nil
}

func (client *Client) Register(ctx context.Context, subject, schema string) (int, error) {
	payload, err := json.Marshal(map[string]string{
		"schema":     schema,
		"schemaType": "AVRO",
	})
	if err != nil {
		return 0, fmt.Errorf("encode schema registration request: %w", err)
	}

	var response schemaResponse
	if err := client.request(ctx, http.MethodPost, path.Join("subjects", subject, "versions"), bytes.NewReader(payload), &response); err != nil {
		return 0, err
	}
	if response.ID <= 0 {
		return 0, fmt.Errorf("Schema Registry returned invalid schema id %d", response.ID)
	}
	return response.ID, nil
}

func (client *Client) SchemaByID(ctx context.Context, id int) (string, error) {
	var response schemaResponse
	if err := client.request(ctx, http.MethodGet, fmt.Sprintf("schemas/ids/%d", id), nil, &response); err != nil {
		return "", err
	}
	if strings.TrimSpace(response.Schema) == "" {
		return "", fmt.Errorf("Schema Registry returned an empty schema for id %d", id)
	}
	return response.Schema, nil
}

func (client *Client) request(ctx context.Context, method, endpoint string, body io.Reader, output any) error {
	requestURL := *client.baseURL
	requestURL.Path = path.Join(client.baseURL.Path, endpoint)

	request, err := http.NewRequestWithContext(ctx, method, requestURL.String(), body)
	if err != nil {
		return fmt.Errorf("create Schema Registry request: %w", err)
	}
	request.Header.Set("Accept", "application/vnd.schemaregistry.v1+json, application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	}
	request.SetBasicAuth(client.username, client.password)

	response, err := client.http.Do(request)
	if err != nil {
		return fmt.Errorf("Schema Registry request %s %s: %w", method, requestURL.String(), err)
	}
	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("Schema Registry request %s %s returned %s: %s", method, requestURL.String(), response.Status, strings.TrimSpace(string(message)))
	}
	if output == nil {
		return nil
	}
	if err := json.NewDecoder(response.Body).Decode(output); err != nil {
		return fmt.Errorf("decode Schema Registry response: %w", err)
	}
	return nil
}
