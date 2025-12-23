package loader

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"

	"time"

	"github.com/tsliwowicz/go-wrk/util"
	"golang.org/x/net/http2"
)

// clientCacheKey 用于标识客户端配置的唯一键
type clientCacheKey struct {
	disableCompression bool
	disableKeepAlive   bool
	skipVerify         bool
	timeoutms          int
	allowRedirects     bool
	clientCert         string
	clientKey          string
	caCert             string
	usehttp2           bool
}

// clientCache 全局HTTP客户端缓存
var (
	clientCache   = make(map[clientCacheKey]*http.Client)
	clientCacheMu sync.RWMutex
)

func client(disableCompression, disableKeepAlive, skipVerify bool, timeoutms int, allowRedirects bool, clientCert, clientKey, caCert string, usehttp2 bool) (*http.Client, error) {
	// 创建缓存键
	key := clientCacheKey{
		disableCompression: disableCompression,
		disableKeepAlive:   disableKeepAlive,
		skipVerify:         skipVerify,
		timeoutms:          timeoutms,
		allowRedirects:     allowRedirects,
		clientCert:         clientCert,
		clientKey:          clientKey,
		caCert:             caCert,
		usehttp2:           usehttp2,
	}

	// 尝试从缓存读取
	clientCacheMu.RLock()
	cachedClient, found := clientCache[key]
	clientCacheMu.RUnlock()

	if found {
		return cachedClient, nil
	}

	// 缓存未命中，创建新客户端
	clientCacheMu.Lock()
	defer clientCacheMu.Unlock()

	// 双重检查，防止并发创建
	if cachedClient, found := clientCache[key]; found {
		return cachedClient, nil
	}

	// 使用优化的客户端创建函数
	client, err := createOptimizedClient(disableCompression, disableKeepAlive, skipVerify,
		timeoutms, allowRedirects, clientCert, clientKey, caCert, usehttp2)
	
	if err != nil {
		return nil, err
	}
	
	clientCache[key] = client
	return client, nil
}
