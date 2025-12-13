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

	client := &http.Client{}
	//overriding the default parameters
	client.Transport = &http.Transport{
		DisableCompression:    disableCompression,
		DisableKeepAlives:     disableKeepAlive,
		ResponseHeaderTimeout: time.Millisecond * time.Duration(timeoutms),
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: skipVerify},
		MaxIdleConnsPerHost:   10000, // 增加每个主机的最大空闲连接数
		MaxConnsPerHost:       10000, // 增加每个主机的最大连接数
		IdleConnTimeout:       90 * time.Second,
	}

	if !allowRedirects {
		//returning an error when trying to redirect. This prevents the redirection from happening.
		client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			return util.NewRedirectError("redirection not allowed")
		}
	}

	if clientCert == "" && clientKey == "" && caCert == "" {
		clientCache[key] = client
		return client, nil
	}

	if clientCert == "" {
		return nil, fmt.Errorf("client certificate can't be empty")
	}

	if clientKey == "" {
		return nil, fmt.Errorf("client key can't be empty")
	}
	cert, err := tls.LoadX509KeyPair(clientCert, clientKey)
	if err != nil {
		return nil, fmt.Errorf("Unable to load cert tried to load %v and %v but got %v", clientCert, clientKey, err)
	}

	// Load our CA certificate
	clientCACert, err := ioutil.ReadFile(caCert)
	if err != nil {
		return nil, fmt.Errorf("Unable to open cert %v", err)
	}

	clientCertPool := x509.NewCertPool()
	clientCertPool.AppendCertsFromPEM(clientCACert)

	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            clientCertPool,
		InsecureSkipVerify: skipVerify,
	}

	tlsConfig.BuildNameToCertificate()
	t := &http.Transport{
		TLSClientConfig:     tlsConfig,
		MaxIdleConnsPerHost: 10000,
		MaxConnsPerHost:     10000,
		IdleConnTimeout:     90 * time.Second,
	}

	if usehttp2 {
		http2.ConfigureTransport(t)
	}
	client.Transport = t
	clientCache[key] = client
	return client, nil
}
