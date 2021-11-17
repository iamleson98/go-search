package es

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch"
	"github.com/elastic/go-elasticsearch/esapi"
	"github.com/iamleson98/go-search/textindexer/index"
)

const indexName = "textindexer"

const batchSize = 10

var esMapping = `
{
	"mappings" : {
		"properties": 	{
			"LinkID": 	 {"type": "keyword"},
			"URL": 		 {"type": "keyword"},
			"Content": 	 {"type": "text"},
			"Title": 	 {"type": "date"},
			"IndexedAt": {"type": "date"},
			"PageRank":  {"type": "double"}
		}
	}
}`

type esSearches struct {
	Hits esSearchResHits `json:"hits"`
}

type esSearchResHits struct {
	Total   esTotal        `json:"total"`
	HitList []esHitWrapper `json:"hits"`
}

type esTotal struct {
	Count uint64 `json:"value"`
}

type esHitWrapper struct {
	DocSource esDoc `json:"_source"`
}

type esDoc struct {
	LinkID    string    `json:"LinkID"`
	URL       string    `json:"URL"`
	Title     string    `json:"Title"`
	Content   string    `json:"Content"`
	IndexedAt time.Time `json:"IndexedAt"`
	PageRank  float64   `json:"PageRank,omitempty"`
}

type esErrorRes struct {
	Error esError `json:"error"`
}

type esUpdateRes struct {
	Error esError `json:"error"`
}

type esError struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
}

func (e esError) Error() string {
	return fmt.Sprintf("%s: %s", e.Type, e.Reason)
}

var _ index.Indexer = (*ElasticSearchIndexer)(nil)

type ElasticSearchIndexer struct {
	es         *elasticsearch.Client
	refreshOpt func(*esapi.UpdateRequest)
}

func NewElasticSearchIndexer(esNodes []string, syncUpdates bool) (*ElasticSearchIndexer, error) {
	cfg := elasticsearch.Config{
		Addresses: esNodes,
	}
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	if err = ensureIndex(es); err != nil {
		return nil, err
	}

	refreshOpt := es.Update.WithRefresh("false")
	if syncUpdates {
		refreshOpt = es.Update.WithRefresh("true")
	}

	return &ElasticSearchIndexer{
		es:         es,
		refreshOpt: refreshOpt,
	}, nil
}

func ensureIndex(es *elasticsearch.Client) error {
	mappingsReader := strings.NewReader(esMapping)
	res, err := es.Indices.Create(indexName, es.Indices.Create.WithBody(mappingsReader))
	if err != nil {
		return fmt.Errorf("cannot create ES index: %w", err)
	} else if res.IsError() {

	}
}

func unmarshalError(res *esapi.Response) error {
	return unmarshalResponse(res, nil)
}

func unmarshalResponse(res *esapi.Response, to interface{}) error {
	defer func() {
		_ = res.Body.Close()
	}()

	if res.IsError() {
		var errRes esErrorRes
		if err := json.NewDecoder(res.Body).Decode(&errRes); err != nil {
			return err
		}

		return errRes.Error
	}

	return json.NewDecoder(res.Body).Decode(to)
}

func mapEsDoc(d *esDoc) *index.Document {
	return &index.Document{}
}
