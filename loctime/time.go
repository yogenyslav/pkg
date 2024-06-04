// Package loctime provides a simple way to get the current time in the location.
package loctime

import (
	"time"
)

var (
	// MoscowLocation Europe/Moscow is UTC+3.
	MoscowLocation = time.FixedZone("Europe/Moscow", 3*60*60)
	// CurLocation is the UTC.
	CurLocation = time.UTC
)

// SetLocation sets the location. Should be called before using GetLocalTime.
func SetLocation(loc *time.Location) {
	CurLocation = loc
}

// GetLocalTime returns the current time in the location set by SetLocation.
func GetLocalTime() time.Time {
	return time.Now().In(CurLocation)
}
