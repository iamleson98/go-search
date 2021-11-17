package index

import (
	"github.com/google/uuid"
)

// Indexer is implemented by objects that can index and search documents discovered
type Indexer interface {
	Index(doc *Document) error
	FindByID(linkID uuid.UUID) (*Document, error)
	Search(query Query) (Iterator, error)
	UpdateScore(linkID uuid.UUID, score float64) error
}

// Iterator is implemented by objects that can paginate search results
type Iterator interface {
	Close() error
	Next() bool
	Error() error
	Document() *Document
	TotalCount() uint64
}

type QueryType uint8

const (
	QueryTypeMatch  QueryType = iota // QueryTypeMatch requests the indexer to match each expression term.
	QueryTypePhrase                  // QueryTypePhrase searches for an exact phrase match.
)

// Query contains a set of parameters to use when searching indexed documents
type Query struct {
	Type       QueryType // the way indexer should interpret the search expression
	Expression string    // the search expression
	Offset     uint64
}
