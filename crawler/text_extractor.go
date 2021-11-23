package crawler

import (
	"context"
	"html"
	"regexp"
	"strings"
	"sync"

	"github.com/iamleson98/go-search/pipeline"
	"github.com/microcosm-cc/bluemonday"
)

var (
	titleRegex         = regexp.MustCompile(`(?i)<title.*?>(.*?)</title>`)
	repeatedSpaceRegex = regexp.MustCompile(`\s+`)
)

type textExtrator struct {
	policyPool sync.Pool
}

func newTextExtrator() *textExtrator {
	return &textExtrator{
		policyPool: sync.Pool{
			New: func() interface{} {
				return bluemonday.StrictPolicy()
			},
		},
	}
}

func (te *textExtrator) Process(ctx context.Context, p pipeline.Payload) (pipeline.Payload, error) {
	payload := p.(*crawlerPayload)
	policy := te.policyPool.Get().(*bluemonday.Policy)

	if titleMatch := titleRegex.FindStringSubmatch(payload.RawContent.String()); len(titleMatch) == 2 {
		payload.Title = strings.TrimSpace(html.UnescapeString(repeatedSpaceRegex.ReplaceAllString(
			policy.Sanitize(titleMatch[1]), " ",
		)))
	}

	payload.TextContent = strings.TrimSpace(html.UnescapeString(repeatedSpaceRegex.ReplaceAllString(
		policy.SanitizeReader(&payload.RawContent).String(), " ",
	)))
	te.policyPool.Put(policy)

	return payload, nil
}
