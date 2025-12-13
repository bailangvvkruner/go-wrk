package loader

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	histo "github.com/HdrHistogram/hdrhistogram-go"
	"github.com/tsliwowicz/go-wrk/util"
)

const (
	USER_AGENT = "go-wrk"
)

type LoadCfg struct {
	duration           int // seconds
	goroutines         int
	testUrl            string
	reqBody            string
	method             string
	host               string
	header             map[string]string
	statsAggregator    chan *RequesterStats
	timeoutms          int
	allowRedirects     bool
	disableCompression bool
	disableKeepAlive   bool
	skipVerify         bool
	interrupted        int32
	clientCert         string
	clientKey          string
	caCert             string
	http2              bool
}

// RequesterStats used for collecting aggregate statistics
type RequesterStats struct {
	TotRespSize int64
	TotDuration time.Duration
	NumRequests int
	NumErrs     int
	ErrMap      map[string]int
	Histogram   *histo.Histogram
}

func NewLoadCfg(duration int, // seconds
	goroutines int,
	testUrl string,
	reqBody string,
	method string,
	host string,
	header map[string]string,
	statsAggregator chan *RequesterStats,
	timeoutms int,
	allowRedirects bool,
	disableCompression bool,
	disableKeepAlive bool,
	skipVerify bool,
	clientCert string,
	clientKey string,
	caCert string,
	http2 bool) (rt *LoadCfg) {
	rt = &LoadCfg{duration, goroutines, testUrl, reqBody, method, host, header, statsAggregator, timeoutms,
		allowRedirects, disableCompression, disableKeepAlive, skipVerify, 0, clientCert, clientKey, caCert, http2}
	return
}

// urlCache URL转义结果缓存
var (
	urlCache   = make(map[string]string)
	urlCacheMu sync.RWMutex
)

// requestPool 请求对象池
var requestPool = sync.Pool{
	New: func() interface{} {
		return &http.Request{}
	},
}

// bufferPool 缓冲区池
var bufferPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 4096))
	},
}

const maxBodySize = 1024 * 1024 // 1MB，限制响应体最大读取大小

func escapeUrlStr(in string) string {
	// 检查缓存
	urlCacheMu.RLock()
	cached, found := urlCache[in]
	urlCacheMu.RUnlock()

	if found {
		return cached
	}

	// 缓存未命中，进行计算
	qm := strings.Index(in, "?")
	if qm != -1 {
		qry := in[qm+1:]
		qrys := strings.Split(qry, "&")

		// 使用strings.Builder提高性能
		var query strings.Builder
		query.Grow(len(in) + 20) // 预分配空间

		first := true
		for _, q := range qrys {
			qSplit := strings.SplitN(q, "=", 2)
			if len(qSplit) == 2 {
				if first {
					first = false
				} else {
					query.WriteString("&")
				}
				query.WriteString(qSplit[0])
				query.WriteString("=")
				query.WriteString(url.QueryEscape(qSplit[1]))
			} else {
				if first {
					first = false
				} else {
					query.WriteString("&")
				}
				query.WriteString(qSplit[0])
			}
		}

		result := in[:qm] + "?" + query.String()

		// 存入缓存
		urlCacheMu.Lock()
		urlCache[in] = result
		urlCacheMu.Unlock()

		return result
	} else {
		// 没有查询参数，直接缓存原字符串
		urlCacheMu.Lock()
		urlCache[in] = in
		urlCacheMu.Unlock()
		return in
	}
}

// DoRequest single request implementation. Returns the size of the response and its duration
// On error - returns -1 on both
func DoRequest(httpClient *http.Client, header map[string]string, method, host, loadUrl, reqBody string) (respSize int, duration time.Duration, err error) {
	respSize = -1
	duration = -1

	loadUrl = escapeUrlStr(loadUrl)

	var requestBody io.Reader
	if len(reqBody) > 0 {
		requestBody = bytes.NewBufferString(reqBody)
	}

	// 创建请求（不使用对象池，因为http.NewRequest内部结构复杂）
	req, err := http.NewRequest(method, loadUrl, requestBody)
	if err != nil {
		return 0, 0, err
	}

	// 设置请求头
	for hk, hv := range header {
		req.Header.Set(hk, hv) // 使用Set而不是Add，避免重复添加
	}

	req.Header.Set("User-Agent", USER_AGENT)
	if host != "" {
		req.Host = host
	}

	start := time.Now()
	resp, err := httpClient.Do(req)
	if err != nil {
		// this is a bit weird. When redirection is prevented, a url.Error is retuned. This creates an issue to distinguish
		// between an invalid URL that was provided and and redirection error.
		_, ok := err.(*url.Error)
		if !ok {
			return 0, 0, err
		}
		return 0, 0, err
	}
	if resp == nil {
		return 0, 0, errors.New("empty response")
	}
	defer func() {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
	}()

	// 使用缓冲区池读取响应体
	responseBuffer := bufferPool.Get().(*bytes.Buffer)
	responseBuffer.Reset()
	defer bufferPool.Put(responseBuffer)

	// 限制读取大小，避免大响应体导致内存问题
	limitedReader := io.LimitReader(resp.Body, maxBodySize)
	bodySize, err := responseBuffer.ReadFrom(limitedReader)
	if err != nil {
		return 0, 0, err
	}

	if resp.StatusCode/100 == 2 { // Treat all 2XX as successful
		duration = time.Since(start)
		respSize = int(bodySize) + int(util.EstimateHttpHeadersSize(resp.Header))
	} else if resp.StatusCode == http.StatusMovedPermanently || resp.StatusCode == http.StatusTemporaryRedirect {
		duration = time.Since(start)
		respSize = int(resp.ContentLength) + int(util.EstimateHttpHeadersSize(resp.Header))
	} else {
		return 0, 0, errors.New(fmt.Sprint("received status code ", resp.StatusCode))
	}

	return
}

func unwrap(err error) error {
	for errors.Unwrap(err) != nil {
		err = errors.Unwrap(err)
	}
	return err
}

// Requester a go function for repeatedly making requests and aggregating statistics as long as required
// When it is done, it sends the results using the statsAggregator channel
func (cfg *LoadCfg) RunSingleLoadSession() {
	stats := &RequesterStats{ErrMap: make(map[string]int), Histogram: histo.New(1, int64(cfg.duration*1000000), 4)}
	start := time.Now()

	httpClient, err := client(cfg.disableCompression, cfg.disableKeepAlive, cfg.skipVerify,
		cfg.timeoutms, cfg.allowRedirects, cfg.clientCert, cfg.clientKey, cfg.caCert, cfg.http2)
	if err != nil {
		log.Fatal(err)
	}

	for time.Since(start).Seconds() <= float64(cfg.duration) && atomic.LoadInt32(&cfg.interrupted) == 0 {
		respSize, reqDur, err := DoRequest(httpClient, cfg.header, cfg.method, cfg.host, cfg.testUrl, cfg.reqBody)
		if err != nil {
			stats.ErrMap[unwrap(err).Error()] += 1
			stats.NumErrs++
		} else if respSize > 0 {
			stats.TotRespSize += int64(respSize)
			stats.TotDuration += reqDur
			stats.Histogram.RecordValue(reqDur.Microseconds())
			stats.NumRequests++
		} else {
			stats.NumErrs++
		}
	}
	cfg.statsAggregator <- stats
}

func (cfg *LoadCfg) Stop() {
	atomic.StoreInt32(&cfg.interrupted, 1)
}
