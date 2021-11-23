package db

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/iamleson98/go-search/linkgraph/graph"
)

var (
	upsertLinkQuery       = `INSERT INTO links (url, retrieved_at) VALUES ($1, $2) ON CONFLICT (url) DO UPDATE SET retrieved_at=GREATEST(links.retrieved_at, $2) RETURNING id, retrieved_at`
	findLinkQuery         = `SELECT url, retrieved_at FROM links WHERE id=$1`
	linksInPartitionQuery = `SELECT id, url, retrieved_at FROM links WHERE id >= $1 AND id < $2 retrieved_at < $3`
	upsertEdgeQuery       = `INSERT INTO edges (src, dst, updated_at) VALUES ($1, $2, NOW()) ON CONFLICT (src, dst) DO UPDATE SET updated_at=NOW() RETURNING id, updated_at`
	edgesInPartitionQuery = `SELECT id, src, dst, updated_at FROM edges WHERE src >= $1 AND src < $2 AND updated_at < $3`
	removeStaleEdgesQuery = `DELETE FROM edges WHERE src=$1 AND updated_at < $2`

	_ graph.Graph = (*DbGraph)(nil)
)

type DbGraph struct {
	db *sql.DB
}

// NewCockroachDbGraph returns a CockroachDbGraph instance that connects to the cockroachdb instance specified by dsn.
func NewCockroachDbGraph(dsn string) (*DbGraph, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	return &DbGraph{db: db}, nil
}

// Close
func (d *DbGraph) Close() error {
	return d.db.Close()
}

// UpsertLink
func (d *DbGraph) UpsertLink(link *graph.Link) error {
	row := d.db.QueryRow(upsertLinkQuery, link.URL, link.RetrievedAt.UTC())
	if err := row.Scan(&link.ID, &link.RetrievedAt); err != nil {
		return fmt.Errorf("upsert link: %w", err)
	}

	link.RetrievedAt = link.RetrievedAt.UTC()
	return nil
}

// FindLink looks up a link bt its ID.
func (d *DbGraph) FindLink(id uuid.UUID) (*graph.Link, error) {
	var (
		row  = d.db.QueryRow(findLinkQuery, id)
		link = &graph.Link{
			ID: id,
		}
	)

	if err := row.Scan(&link.URL, &link.RetrievedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("find link: %w", graph.ErrNotFound)
		}

		return nil, fmt.Errorf("find link: %w", err)
	}

	link.RetrievedAt = link.RetrievedAt.UTC()
	return link, nil
}

// Links returns an iterator for the set of links whose IDs belong to the [fromID, toID) range were last accessed before provideed value
func (d *DbGraph) Links(fromID, toID uuid.UUID, accessedBefore time.Time) (graph.LinkIterator, error) {
	rows, err := d.db.Query(linksInPartitionQuery, fromID, toID, accessedBefore.UTC())
	if err != nil {
		return nil, fmt.Errorf("links: %w", err)
	}

	return &linkIterator{
		rows: rows,
	}, nil
}

func (d *DbGraph) UpsertEdge(edge *graph.Edge) error {
	row := d.db.QueryRow(upsertEdgeQuery, edge.Src, edge.Dst)
	if err := row.Scan(&edge.ID, &edge.UpdatedAt); err != nil {
		if isForeignKeyViolationError(err) {
			err = graph.ErrUnknownEdgeLinks
		}
		return fmt.Errorf("upsert edge: %w", err)
	}

	edge.UpdatedAt = edge.UpdatedAt.UTC()
	return nil
}

func (d *DbGraph) Edges(fromID, toID uuid.UUID, updatedBefore time.Time) (graph.EdgeIterator, error) {
	rows, err := d.db.Query(edgesInPartitionQuery, fromID, toID, updatedBefore.UTC())
	if err != nil {
		return nil, fmt.Errorf("edges: %w", err)
	}

	return &edgeIterator{rows: rows}, nil
}

func (d *DbGraph) RemoveStaleEdges(fromID uuid.UUID, updatedBefore time.Time) error {
	_, err := d.db.Exec(removeStaleEdgesQuery, fromID, updatedBefore.UTC())
	if err != nil {
		return fmt.Errorf("remove stale edges: %w", err)
	}

	return nil
}
