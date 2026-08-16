package urnettools

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestParseRelease(t *testing.T) {
	data := []byte(`{"tag_name": "v3.23.0", "published_at": "2025-01-02T03:04:05Z", "name": "ignored"}`)
	release, err := ParseRelease(data)
	if err != nil {
		t.Fatal(err)
	}
	if release.TagName != "v3.23.0" {
		t.Errorf("TagName = %q, want v3.23.0", release.TagName)
	}
	want := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	if !release.PublishedAt.Equal(want) {
		t.Errorf("PublishedAt = %v, want %v", release.PublishedAt, want)
	}
}

func TestParseReleaseMissingTag(t *testing.T) {
	if _, err := ParseRelease([]byte(`{"published_at": "2025-01-02T03:04:05Z"}`)); err == nil {
		t.Error("missing tag_name should error")
	}
}

func TestParseReleaseInvalidDate(t *testing.T) {
	if _, err := ParseRelease([]byte(`{"tag_name": "v1", "published_at": "yesterday"}`)); err == nil {
		t.Error("invalid published_at should error")
	}
}

func TestHTTPReleaseClientLatest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/releases/latest" {
			t.Errorf("path = %q, want /releases/latest", r.URL.Path)
		}
		if r.Header.Get("Accept") != "application/vnd.github+json" {
			t.Errorf("Accept = %q", r.Header.Get("Accept"))
		}
		w.Write([]byte(`{"tag_name": "v3.23.0", "published_at": "2025-01-02T03:04:05Z"}`))
	}))
	defer server.Close()
	client := NewHTTPReleaseClient(server.URL)
	release, err := client.Latest(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if release.TagName != "v3.23.0" {
		t.Errorf("TagName = %q, want v3.23.0", release.TagName)
	}
}

func TestHTTPReleaseClientError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	client := NewHTTPReleaseClient(server.URL)
	if _, err := client.Latest(context.Background()); err == nil {
		t.Error("a 500 response should error")
	}
}

func TestCheckUpdateUpToDate(t *testing.T) {
	install := &Install{Path: t.TempDir()}
	if err := os.WriteFile(install.DatePath(), []byte("2025-01-02T03:04:05Z\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(install.VersionPath(), []byte("v3.23.0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	releases := &fakeReleaseClient{latest: &Release{TagName: "v3.23.0", PublishedAt: time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)}}
	tool, stdout, _ := newTestTool(install, &fakePlatform{}, releases, &fakeRunner{}, "")
	tag, err := tool.CheckUpdate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if tag != "" {
		t.Errorf("tag = %q, want empty", tag)
	}
	if stdout.String() != "Installed version is up-to-date (v3.23.0)\n" {
		t.Errorf("stdout = %q", stdout.String())
	}
}

func TestCheckUpdateAvailable(t *testing.T) {
	install := &Install{Path: t.TempDir()}
	if err := os.WriteFile(install.DatePath(), []byte("2025-01-02T03:04:05Z\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(install.VersionPath(), []byte("v3.23.0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	releases := &fakeReleaseClient{latest: &Release{TagName: "v3.24.0", PublishedAt: time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)}}
	tool, stdout, _ := newTestTool(install, &fakePlatform{}, releases, &fakeRunner{}, "")
	tag, err := tool.CheckUpdate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if tag != "v3.24.0" {
		t.Errorf("tag = %q, want v3.24.0", tag)
	}
	if stdout.String() != "Update available (v3.24.0)\n" {
		t.Errorf("stdout = %q", stdout.String())
	}
}

func TestCheckUpdateMissingDateFile(t *testing.T) {
	install := &Install{Path: t.TempDir()}
	releases := &fakeReleaseClient{latest: &Release{TagName: "v3.24.0", PublishedAt: time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)}}
	tool, _, _ := newTestTool(install, &fakePlatform{}, releases, &fakeRunner{}, "")
	_, err := tool.CheckUpdate(context.Background())
	if err == nil || err.Error() != "Cannot read the installation date file at "+install.DatePath() {
		t.Errorf("err = %v", err)
	}
}

func TestCheckUpdateFetchError(t *testing.T) {
	install := &Install{Path: t.TempDir()}
	releases := &fakeReleaseClient{err: errors.New("network down")}
	tool, _, _ := newTestTool(install, &fakePlatform{}, releases, &fakeRunner{}, "")
	_, err := tool.CheckUpdate(context.Background())
	want := "Failed to fetch release information from GitHub API. Are you sure the version exists and your internet connection is working?"
	if err == nil || err.Error() != want {
		t.Errorf("err = %q, want %q", err, want)
	}
}
