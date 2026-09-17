package urnettools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// GithubAPIBase is the PS1's $GithubURLBase (urnet-tools.ps1:81). Exported
// so cmd/urnet-tools/main.go can build the default release client.
const GithubAPIBase = "https://api.github.com/repos/urnetwork/connect"

// githubAPITimeout bounds every GitHub API call made by the release client.
const githubAPITimeout = 30 * time.Second

// Release is the minimal GitHub release shape the tool reads: the tag name
// and the publication time, which drives the date-based update comparison.
type Release struct {
	TagName     string
	PublishedAt time.Time
}

// ParseRelease decodes a GitHub release JSON body. Only tag_name and
// published_at are needed.
func ParseRelease(data []byte) (*Release, error) {
	var raw struct {
		TagName     string `json:"tag_name"`
		PublishedAt string `json:"published_at"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	if raw.TagName == "" {
		return nil, errors.New("release JSON has no tag_name")
	}
	publishedAt, err := time.Parse(time.RFC3339, raw.PublishedAt)
	if err != nil {
		return nil, fmt.Errorf("release JSON has an invalid published_at: %w", err)
	}
	return &Release{
		TagName:     raw.TagName,
		PublishedAt: publishedAt,
	}, nil
}

// ReleaseClient fetches release metadata. The only consumer is CheckUpdate,
// which needs the latest release; ByTag is deliberately not part of the
// surface (drop unused interface methods).
type ReleaseClient interface {
	Latest(ctx context.Context) (*Release, error)
}

// NewHTTPReleaseClient returns a ReleaseClient backed by real HTTP calls
// against the GitHub API.
func NewHTTPReleaseClient(baseURL string) ReleaseClient {
	return &httpReleaseClient{
		baseURL: baseURL,
		client:  &http.Client{Timeout: githubAPITimeout},
	}
}

type httpReleaseClient struct {
	baseURL string
	client  *http.Client
}

// Latest returns the newest published release, or the section 6.0 error
// message on any failure (non-2xx, empty body, network error, parse error).
func (self *httpReleaseClient) Latest(ctx context.Context) (*Release, error) {
	reqCtx, cancel := context.WithTimeout(ctx, githubAPITimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, self.baseURL+"/releases/latest", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	resp, err := self.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || len(body) == 0 {
		return nil, errors.New("non-2xx or empty release response")
	}
	return ParseRelease(body)
}
