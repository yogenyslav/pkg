package loctime

import (
	"time"
)

var (
	MoscowLocation = time.FixedZone("Europe/Moscow", 3*60*60)
	CurLocation    = time.UTC
)

func SetLocation(loc *time.Location) {
	CurLocation = loc
}

func GetLocalTime() time.Time {
	return time.Now().In(CurLocation)
}
