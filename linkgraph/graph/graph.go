package graph

import (
	"time"

	"github.com/google/uuid"
)

// Iterator is implemented by graph objects that can be iterated
type Iterator interface {
	Next() bool
	Error() error
	Close() error
}

// Link
type Link struct {
	ID         uuid.UUID
	URL        string
	RetrivedAt time.Time
}

type Edge struct {
	ID        uuid.UUID
	Src       uuid.UUID // the origin link.
	Dst       uuid.UUID // the destination link.
	UpdatedAt time.Time
}

type LinkIterator interface {
	Iterator
	Link() *Link
}

type EdgeIterator interface {
	Iterator
	Edge() *Edge
}

// Graph is implemented by objects that can mutate or query a link graph
type Graph interface {
	UpsertLink(link *Link) error
	FindLink(id uuid.UUID) (*Link, error)
	Links(fromID, toID uuid.UUID, retrievedBefore time.Time) (LinkIterator, error)
	UpsertEdge(edge *Edge) error
	Edges(fromID, toID uuid.UUID, updatedBefore time.Time) (EdgeIterator, error)
	RemoveStaleEdges(fromID uuid.UUID, updatedBefore time.Time) error
}
