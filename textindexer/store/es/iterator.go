package es

import (
	"github.com/elastic/go-elasticsearch"
)

type esIterator struct {
	es        *elasticsearch.Client
	searchReq map[string]interface{}

	cumIdx uint64
	rsIdx  int
}
