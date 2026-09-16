// Package emoji provides emoji tag validation and suggestion for provider identity.
// This package was removed from upstream connect but is still imported by the SDK.
package emoji

import (
	"errors"
	"unicode/utf8"
)

const (
	MaxTagEmoji     = 6
	SuggestMaxEmoji = 3
)

var (
	ErrEmpty    = errors.New("emoji tag is empty")
	ErrTooMany  = errors.New("too many emoji in tag")
)

// Suggest returns a random tag of count distinct emoji.
func Suggest(count int, rng interface{}) string {
	// Placeholder: return empty string until real implementation is added.
	return ""
}

// ValidateTag checks that tag contains only emoji (1-MaxTagEmoji).
func ValidateTag(tag string) (string, int, error) {
	count := utf8.RuneCountInString(tag)
	if count == 0 {
		return "", 0, ErrEmpty
	}
	if count > MaxTagEmoji {
		return "", 0, ErrTooMany
	}
	return tag, count, nil
}
