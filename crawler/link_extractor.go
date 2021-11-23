package crawler

import (
	"context"
	"net/url"
	"regexp"

	"github.com/iamleson98/go-search/pipeline"
)

var (
	exclusionRegex = regexp.MustCompile(`(?i)\.(?:jpg|jpeg|png|gif|ico|css|js)$`)
	baseHrefRegex  = regexp.MustCompile(`(?i)<base.*?href\s*?=\s*?"(.*?)\s*?"`)
	findLinkRegex  = regexp.MustCompile(`(?i)<a.*?href\s*?=\s*?"\s*?(.*?)\s*?".*?>`)
	nofollowRegex  = regexp.MustCompile(`(?i)rel\s*?=\s*?"?nofollow"?`)
)

type linkExtractor struct {
	netDetector PrivateNetworkDetector
}

func newLinkExtractor(netDetector PrivateNetworkDetector) *linkExtractor {
	return &linkExtractor{
		netDetector: netDetector,
	}
}

func (le *linkExtractor) Process(ctx context.Context, p pipeline.Payload) (pipeline.Payload, error) {
	payload := p.(*crawlerPayload)

	relTo, err := url.Parse(payload.URL)
	if err != nil {
		return nil, err
	}

	content := payload.RawContent.String()

	if baseMatch := baseHrefRegex.FindStringSubmatch(content); len(baseMatch) == 2 {
		if base := resolveURL(relTo, ensureHasTrailingSlash(baseMatch[1])); base != nil {
			relTo = base
		}
	}

	seenMap := make(map[string]struct{})
	for _, match := range findLinkRegex.FindAllStringSubmatch(content, -1) {
		link := resolveURL(relTo, match[1])
		if !le.retainLink(relTo.Hostname(), link) {
			continue
		}

		link.Fragment = ""
		linkStr := link.String()
		if _, seen := seenMap[linkStr]; seen {
			continue
		}

		if exclusionRegex.MatchString(linkStr) {
			continue
		}

		if exclusionRegex.MatchString(linkStr) {
			continue
		}

		seenMap[linkStr] = struct{}{}
		if nofollowRegex.MatchString(match[0]) {
			payload.NoFollowLinks = append(payload.NoFollowLinks, linkStr)
		} else {
			payload.Links = append(payload.Links, linkStr)
		}
	}

	return payload, nil
}

func (le *linkExtractor) retainLink(srcHost string, link *url.URL) bool {
	if link == nil {
		return false
	}

	if link.Scheme != "http" && link.Scheme != "https" {
		return false
	}

	if link.Hostname() == srcHost {
		return true
	}

	if isPrivate, err := le.netDetector.IsPrivate(link.Host); err != nil || isPrivate {
		return false
	}

	return true
}

func ensureHasTrailingSlash(s string) string {
	if s[len(s)-1] != '/' {
		return s + "/"
	}
	return s
}

func resolveURL(relTO *url.URL, target string) *url.URL {
	tLen := len(target)

	if tLen == 0 {
		return nil
	}

	if tLen >= 1 && target[0] == '/' {
		if tLen >= 2 && target[1] == '/' {
			target = relTO.Scheme + ":" + target
		}
	}

	if targetURL, err := url.Parse(target); err == nil {
		return relTO.ResolveReference(targetURL)
	}

	return nil
}
