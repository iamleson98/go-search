package index

import (
	"time"

	"github.com/google/uuid"
)

// Document describes a web page whose content has been indexed by this service
type Document struct {
	LinkID    uuid.UUID
	URL       string
	Title     string
	Content   string
	IndexedAt time.Time
	PageRank  float64
}
