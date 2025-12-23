package loader

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	histo "github.com/HdrHistogram/hdrhistogram-go"
	"golang.org/x/net/http2"
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

// 优化1：无锁URL缓存 - 使用线程局部存储+全局缓存
var (
	// 全局缓存（带锁）
	globalURLCache   = make(map[string]string)
	globalURLCacheMu sync.RWMutex

	// 线程局部缓存池（无锁）
	threadLocalCachePool = sync.Pool{
		New: func() interface{} {
			return make(map[string]string)
		},
	}
)

// 优化2：请求对象池
var requestPool = sync.Pool{
	New: func() interface{} {
		return &http.Request{
			Header: make(http.Header),
		}
	},
}

// 优化3：缓冲区池
var bufferPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 4096))
	},
}

const maxBodySize = 1024 * 1024 // 1MB

// 优化后的URL转义函数
func escapeUrlStr(in string) string {
	// 1. 尝试从线程局部缓存获取
	threadCache := threadLocalCachePool.Get().(map[string]string)
	cached, found := threadCache[in]
	if found {
		threadLocalCachePool.Put(threadCache)
		return cached
	}

	// 2. 尝试从全局缓存获取（读锁）
	globalURLCacheMu.RLock()
	cached, found = globalURLCache[in]
	globalURLCacheMu.RUnlock()
	
	if found {
		// 存入线程局部缓存以便下次快速访问
		threadCache[in] = cached
		threadLocalCachePool.Put(threadCache)
		return cached
	}

	// 3. 缓存未命中，进行计算
	var result string
	qm := strings.Index(in, "?")
	if qm != -1 {
		qry := in[qm+1:]
		qrys := strings.Split(qry, "&")

		var query strings.Builder
		query.Grow(len(in) + 20)

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
		result = in[:qm] + "?" + query.String()
	} else {
		result = in
	}

	// 4. 更新缓存（先更新全局，再更新线程局部）
	globalURLCacheMu.Lock()
	globalURLCache[in] = result
	globalURLCacheMu.Unlock()

	threadCache[in] = result
	threadLocalCachePool.Put(threadCache)

	return result
}

// 优化4：客户端连接池配置（支持TLS证书）
func createOptimizedClient(disableCompression, disableKeepAlive, skipVerify bool,
	timeoutms int, allowRedirects bool, clientCert, clientKey, caCert string, usehttp2 bool) (*http.Client, error) {
	
	// 创建基本传输配置
	transport := &http.Transport{
		DisableCompression:    disableCompression,
		DisableKeepAlives:     disableKeepAlive,
		MaxIdleConnsPerHost:   100,           // 增加每个主机空闲连接数
		IdleConnTimeout:       90 * time.Second, // 延长空闲连接超时
		ExpectContinueTimeout: 1 * time.Second,
	}
	
	// 配置TLS
	if skipVerify {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	} else {
		transport.TLSClientConfig = nil
	}
	
	// 处理客户端证书
	if clientCert != "" || clientKey != "" || caCert != "" {
		if clientCert == "" {
			return nil, fmt.Errorf("client certificate can't be empty")
		}
		if clientKey == "" {
			return nil, fmt.Errorf("client key can't be empty")
		}
		
		// 加载客户端证书
		cert, err := tls.LoadX509KeyPair(clientCert, clientKey)
		if err != nil {
			return nil, fmt.Errorf("Unable to load cert tried to load %v and %v but got %v", clientCert, clientKey, err)
		}
		
		var tlsConfig *tls.Config
		if skipVerify {
			tlsConfig = &tls.Config{
				Certificates:       []tls.Certificate{cert},
				InsecureSkipVerify: true,
			}
		} else {
			tlsConfig = &tls.Config{
				Certificates: []tls.Certificate{cert},
			}
			
			// 如果提供了CA证书，加载它
			if caCert != "" {
				clientCACert, err := ioutil.ReadFile(caCert)
				if err != nil {
					return nil, fmt.Errorf("Unable to open cert %v", err)
				}
				clientCertPool := x509.NewCertPool()
				clientCertPool.AppendCertsFromPEM(clientCACert)
				tlsConfig.RootCAs = clientCertPool
			}
		}
		
		// 创建新的传输配置
		t := &http.Transport{
			TLSClientConfig:       tlsConfig,
			DisableCompression:    disableCompression,
			DisableKeepAlives:     disableKeepAlive,
			MaxIdleConnsPerHost:   100,
			IdleConnTimeout:       90 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		}
		
		// 配置HTTP/2
		if usehttp2 {
			http2.ConfigureTransport(t)
		}
		transport = t
	} else if usehttp2 {
		// 如果没有证书但需要HTTP/2
		http2.ConfigureTransport(transport)
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   time.Duration(timeoutms) * time.Millisecond,
	}
	
	if !allowRedirects {
		client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		}
	}
	
	return client, nil
}

// 优化后的请求函数
func DoRequest(httpClient *http.Client, header map[string]string, method, host, loadUrl, reqBody string) (respSize int, duration time.Duration, err error) {
	respSize = -1
	duration = -1

	loadUrl = escapeUrlStr(loadUrl)

	// 从对象池获取请求对象
	req := requestPool.Get().(*http.Request)
	defer requestPool.Put(req)

	// 重置请求对象
	req.Method = method
	req.URL, err = url.Parse(loadUrl)
	if err != nil {
		return 0, 0, err
	}
	
	// 设置请求体
	if len(reqBody) > 0 {
		req.Body = io.NopCloser(bytes.NewBufferString(reqBody))
		req.ContentLength = int64(len(reqBody))
	} else {
		req.Body = nil
		req.ContentLength = 0
	}
	
	// 设置请求头（复用Header map）
	for k := range req.Header {
		delete(req.Header, k)
	}
	for hk, hv := range header {
		req.Header.Set(hk, hv)
	}
	req.Header.Set("User-Agent", USER_AGENT)
	
	if host != "" {
		req.Host = host
	}

	start := time.Now()
	resp, err := httpClient.Do(req)
	if err != nil {
		// 对于重定向错误，返回0大小和持续时间以便统计
		if _, ok := err.(*url.Error); ok && !strings.Contains(err.Error(), "redirect") {
			return 0, 0, err
		}
		// 重定向错误仍然记录持续时间
		duration = time.Since(start)
		return 0, duration, nil
	}
	if resp == nil {
		return 0, 0, errors.New("empty response")
	}
	defer resp.Body.Close()

	// 使用缓冲区池读取响应体
	responseBuffer := bufferPool.Get().(*bytes.Buffer)
	responseBuffer.Reset()
	defer bufferPool.Put(responseBuffer)

	limitedReader := io.LimitReader(resp.Body, maxBodySize)
	bodySize, err := responseBuffer.ReadFrom(limitedReader)
	if err != nil {
		return 0, 0, err
	}

	if resp.StatusCode/100 == 2 { // 所有2XX视为成功
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

// 优化的负载生成会话
func (cfg *LoadCfg) RunSingleLoadSession() {
	// 优化5：为每个goroutine创建独立的统计对象，减少锁竞争
	stats := &RequesterStats{ErrMap: make(map[string]int), Histogram: histo.New(1, int64(cfg.duration*1000000), 4)}
	start := time.Now()

	// 使用优化的客户端创建函数
	httpClient, err := createOptimizedClient(cfg.disableCompression, cfg.disableKeepAlive, cfg.skipVerify,
		cfg.timeoutms, cfg.allowRedirects, cfg.clientCert, cfg.clientKey, cfg.caCert, cfg.http2)
	if err != nil {
		log.Fatal(err)
	}

	// 主请求循环
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
	
	// 发送统计结果
	cfg.statsAggregator <- stats
}

func (cfg *LoadCfg) Stop() {
	atomic.StoreInt32(&cfg.interrupted, 1)
}
