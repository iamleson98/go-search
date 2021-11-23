package crawler

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/iamleson98/go-search/pipeline"
)

var (
	_ pipeline.Payload = (*crawlerPayload)(nil)

	payloadPool = sync.Pool{
		New: func() interface{} {
			return new(crawlerPayload)
		},
	}
)

type crawlerPayload struct {
	LinkID      uuid.UUID
	URL         string
	RetrievedAt time.Time
	RawContent  bytes.Buffer

	NoFollowLinks []string
	Links         []string
	Title         string
	TextContent   string
}

func (p *crawlerPayload) Clone() pipeline.Payload {
	newP := payloadPool.Get().(*crawlerPayload)
	newP.LinkID = p.LinkID
	newP.URL = p.URL
	newP.RetrievedAt = p.RetrievedAt
	newP.NoFollowLinks = append([]string(nil), p.NoFollowLinks...)
	newP.Links = append([]string(nil), p.Links...)
	newP.Title = p.Title
	newP.TextContent = p.TextContent

	_, err := io.Copy(&newP.RawContent, &p.RawContent)
	if err != nil {
		panic(fmt.Sprintf("[BUG] error cloning payload raw content: %v", err))
	}

	return newP
}

func (p *crawlerPayload) MarkAsProcessed() {
	p.URL = p.URL[:0]
	p.RawContent.Reset()
	p.NoFollowLinks = p.NoFollowLinks[:0]
	p.Links = p.Links[:0]
	p.Title = p.Title[:0]
	p.TextContent = p.TextContent[:0]

	payloadPool.Put(p)
}
