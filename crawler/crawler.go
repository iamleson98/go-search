package crawler

import (
	"context"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/iamleson98/go-search/linkgraph/graph"
	"github.com/iamleson98/go-search/pipeline"
	"github.com/iamleson98/go-search/textindexer/index"
)

type URLGetter interface {
	Get(url string) (*http.Response, error)
}

// PrivateNetworkDetector is implemented by objects that can detect whether a host resolves to a private network address
type PrivateNetworkDetector interface {
	IsPrivate(host string) (bool, error)
}

type Graph interface {
	UpsertLink(link *graph.Link) error
	UpsertEdge(edge *graph.Edge) error
	RemoveStaleEdges(fromID uuid.UUID, updateBefore time.Time) error
}

// Indexer is implemented by objects that can index the contents of web-pages retrieved by the crawler pipeline.
type Indexer interface {
	Index(doc *index.Document) error
}

type Config struct {
	PrivateNetworkDetector PrivateNetworkDetector
	URLGetter              URLGetter
	Graph                  Graph
	Indexer                Indexer
	FetchWorkers           int
}

type Crawler struct {
	p *pipeline.Pipeline
}

func NewCrawler(cfg Config) *Crawler {
	return &Crawler{
		p: assembleCrawlerPipeline(cfg),
	}
}

func assembleCrawlerPipeline(cfg Config) *pipeline.Pipeline {
	return pipeline.New(
		pipeline.FixedWorkerPool(
			newLinkFetcher(cfg.URLGetter, cfg.PrivateNetworkDetector),
			cfg.FetchWorkers,
		),
		pipeline.FIFO(newLinkExtractor(cfg.PrivateNetworkDetector)),
		pipeline.FIFO(newTextExtrator()),
		pipeline.Broadcast(
			newGraphUpdater(cfg.Graph),
			newTextIndexer(cfg.Indexer),
		),
	)
}

func (c *Crawler) Crawl(ctx context.Context, linkIt graph.LinkIterator) (int, error) {
	sink := new(countingSink)
	err := c.p.Process(ctx, &linkSource{linkIt: linkIt}, sink)
	return sink.getCount(), err
}

type linkSource struct {
	linkIt graph.LinkIterator
}

func (ls *linkSource) Error() error {
	return ls.linkIt.Error()
}

func (ls *linkSource) Next(context.Context) bool {
	return ls.linkIt.Next()
}

func (ls *linkSource) Payload() pipeline.Payload {
	link := ls.linkIt.Link()
	p := payloadPool.Get().(*crawlerPayload)

	p.LinkID = link.ID
	p.URL = link.URL
	p.RetrievedAt = link.RetrievedAt

	return p
}

type countingSink struct {
	count int
}

func (s *countingSink) Consume(_ context.Context, p pipeline.Payload) error {
	s.count++
	return nil
}

func (s *countingSink) getCount() int {
	return s.count / 2
}
