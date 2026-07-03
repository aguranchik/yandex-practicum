package hdfs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	pathpkg "path"
	"strings"
	"time"
)

type Client struct {
	baseURL    *url.URL
	httpClient *http.Client
	user       string
}

type FileStatus struct {
	PathSuffix string `json:"pathSuffix"`
	Type       string `json:"type"`
	Length     int64  `json:"length"`
}

func NewClient(baseURL, user string) (*Client, error) {
	parsed, err := url.Parse(strings.TrimRight(baseURL, "/"))
	if err != nil {
		return nil, fmt.Errorf("parse WebHDFS URL: %w", err)
	}
	return &Client{
		baseURL: parsed,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		user: user,
	}, nil
}

func (c *Client) Mkdirs(ctx context.Context, hdfsPath string) error {
	requestURL := c.operationURL(hdfsPath, "MKDIRS")
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, requestURL, nil)
	if err != nil {
		return err
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("create HDFS directory %s: %w", hdfsPath, err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return responseError("create HDFS directory", response)
	}
	return nil
}

func (c *Client) Create(ctx context.Context, hdfsPath string, data []byte) error {
	requestURL := c.operationURL(hdfsPath, "CREATE") + "&overwrite=true"
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, requestURL, bytes.NewReader(data))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/octet-stream")
	response, err := c.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("write HDFS file %s: %w", hdfsPath, err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return responseError("write HDFS file", response)
	}
	return nil
}

func (c *Client) ListStatus(ctx context.Context, hdfsPath string) ([]FileStatus, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.operationURL(hdfsPath, "LISTSTATUS"), nil)
	if err != nil {
		return nil, err
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("list HDFS path %s: %w", hdfsPath, err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, responseError("list HDFS path", response)
	}
	var payload struct {
		FileStatuses struct {
			FileStatus []FileStatus `json:"FileStatus"`
		} `json:"FileStatuses"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode HDFS listing: %w", err)
	}
	return payload.FileStatuses.FileStatus, nil
}

func (c *Client) Open(ctx context.Context, hdfsPath string) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.operationURL(hdfsPath, "OPEN"), nil)
	if err != nil {
		return nil, err
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("open HDFS file %s: %w", hdfsPath, err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, responseError("open HDFS file", response)
	}
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("read HDFS file: %w", err)
	}
	return data, nil
}

func (c *Client) operationURL(hdfsPath, operation string) string {
	copyURL := *c.baseURL
	copyURL.Path = pathpkg.Join(copyURL.Path, "/webhdfs/v1", hdfsPath)
	query := copyURL.Query()
	query.Set("op", operation)
	query.Set("user.name", c.user)
	copyURL.RawQuery = query.Encode()
	return copyURL.String()
}

func responseError(action string, response *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
	return fmt.Errorf("%s: HTTP %s: %s", action, response.Status, strings.TrimSpace(string(body)))
}
