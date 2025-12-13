# go-wrk 深度优化方案

## 问题确认
- 目标服务器：能处理25,000 QPS（wrk已验证）
- go-wrk：只能达到~11,500 QPS
- 结论：go-wrk存在性能瓶颈

## 根本原因分析

### 1. Go运行时开销
- **垃圾回收**：频繁内存分配导致GC停顿
- **调度器**：goroutine上下文切换开销
- **边界检查**：数组和切片边界检查
- **接口调用**：动态分发开销

### 2. 网络栈效率
- **标准库抽象**：net/http包的多层抽象
- **连接管理**：HTTP客户端连接复用效率
- **请求构建**：每次请求创建新对象

### 3. 并发模型限制
- **goroutine开销**：虽然轻量，但仍有成本
- **通道通信**：统计收集使用通道，有锁竞争
- **内存局部性**：数据分散影响缓存效率

## 深度优化策略

### 阶段1：激进编译优化

#### 1.1 极致编译参数
```bash
# 极致优化构建
CGO_ENABLED=0 go build \
  -tags netgo,osusergo,static_build \
  -ldflags="-s -w -linkmode=external -extldflags '-static' -X main.version=extreme" \
  -gcflags="all=-B -l=4 -d=checkptr=0" \
  -buildmode=pie \
  -trimpath \
  -o go-wrk-extreme
```

#### 1.2 CPU架构优化
```bash
# 针对特定CPU优化
go build -gcflags="-march=native -mtune=native" -o go-wrk-native
```

#### 1.3 PGO优化
```bash
# 收集性能数据
./go-wrk -c 1000 -d 60 http://localhost:8080 -cpuprofile=cpu.pprof

# PGO构建
go build -pgo=cpu.pprof -o go-wrk-pgo
```

### 阶段2：网络栈优化

#### 2.1 自定义HTTP传输层
```go
// 替代标准库的http.Transport
type OptimizedTransport struct {
    dialer *net.Dialer
    connPool map[string][]net.Conn
    mu sync.RWMutex
}

func (t *OptimizedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
    // 实现更高效的请求处理
    // 减少内存分配，优化连接复用
}
```

#### 2.2 零拷贝请求构建
```go
// 复用请求对象，避免分配
var requestPool = sync.Pool{
    New: func() interface{} {
        return &http.Request{
            Header: make(http.Header),
        }
    },
}

func getRequest() *http.Request {
    req := requestPool.Get().(*http.Request)
    req.Header.Reset()
    return req
}

func putRequest(req *http.Request) {
    requestPool.Put(req)
}
```

#### 2.3 批量请求处理
```go
// 批量发送请求，减少系统调用
func sendBatch(client *http.Client, requests []*http.Request) []*http.Response {
    // 使用pipeline或multiplexing
}
```

### 阶段3：内存管理优化

#### 3.1 对象池全面应用
```go
// 缓冲区池
var bufferPool = sync.Pool{
    New: func() interface{} {
        return make([]byte, 0, 8192)
    },
}

// 响应体池
var responsePool = sync.Pool{
    New: func() interface{} {
        return &http.Response{}
    },
}

// 统计对象池
var statsPool = sync.Pool{
    New: func() interface{} {
        return &loader.RequesterStats{}
    },
}
```

#### 3.2 栈分配优化
```go
// 使用栈分配替代堆分配
func processRequest() {
    // 小对象使用栈分配
    var buf [1024]byte
    // 而不是：buf := make([]byte, 1024)
}
```

#### 3.3 内存对齐
```go
// 优化数据结构内存布局
type OptimizedStats struct {
    NumRequests uint64
    NumErrs     uint64
    TotRespSize uint64
    TotDuration int64
    // 8字节对齐
    _ [4]byte // padding
}
```

### 阶段4：并发模型优化

#### 4.1 无锁统计收集
```go
// 使用原子操作替代通道
type AtomicStats struct {
    requests atomic.Uint64
    errors   atomic.Uint64
    bytes    atomic.Uint64
}

// 每个goroutine本地统计，定期合并
type LocalStats struct {
    requests uint64
    errors   uint64
    bytes    uint64
}
```

#### 4.2 工作窃取调度
```go
// 实现work-stealing提高CPU利用率
type WorkStealingScheduler struct {
    queues []*deque.Deque
    mu     []sync.Mutex
}

func (s *WorkStealingScheduler) Schedule(task func()) {
    // 工作窃取算法
}
```

#### 4.3 CPU亲和性
```go
// 绑定goroutine到特定CPU核心
func setCPUAffinity() {
    runtime.LockOSThread()
    // 设置CPU亲和性
}
```

### 阶段5：系统调用优化

#### 5.1 批量系统调用
```go
// 使用sendmmsg/recvmmsg批量处理
func batchSocketOperations(fds []int) {
    // 批量读写socket
}
```

#### 5.2 内核旁路（可选）
```go
// 使用DPDK或io_uring
// 需要系统级支持
```

## 实施计划

### 第1周：编译和基础优化
1. 实施极致编译参数
2. 添加全面对象池
3. 优化数据结构内存布局

### 第2周：网络栈优化
1. 实现自定义HTTP传输层
2. 优化连接管理
3. 实现批量请求处理

### 第3周：并发模型优化
1. 实现无锁统计收集
2. 优化goroutine调度
3. 添加CPU亲和性支持

### 第4周：系统级优化
1. 优化系统调用
2. 性能测试和调优
3. 稳定性验证

## 预期效果

### 性能目标
| 优化阶段 | 目标QPS | 提升比例 |
|---------|---------|----------|
| 当前 | 11,500 | 基准 |
| 阶段1完成 | 14,000-16,000 | 20-40% |
| 阶段2完成 | 18,000-20,000 | 55-75% |
| 阶段3完成 | 21,000-23,000 | 80-100% |
| 阶段4完成 | 23,000-25,000 | 100-120% |

### 资源优化
- **内存分配减少**：70-80%
- **GC停顿减少**：50-70%
- **CPU使用率优化**：提高30-50%

## 验证方法

### 性能测试
```bash
# 对比测试
wrk -t1 -c1000 -d30s http://localhost:8080
./go-wrk-extreme -c 1000 -d 30 http://localhost:8080

# 不同并发数测试
for c in 100 500 1000 2000 5000; do
    echo "并发数: $c"
    ./go-wrk-extreme -c $c -d 10 http://localhost:8080
done
```

### 性能分析
```bash
# CPU profiling
./go-wrk-extreme -c 1000 -d 30 http://localhost:8080 -cpuprofile=cpu.pprof

# 内存 profiling
./go-wrk-extreme -c 1000 -d 30 http://localhost:8080 -memprofile=mem.pprof

# 阻塞 profiling
./go-wrk-extreme -c 1000 -d 30 http://localhost:8080 -blockprofile=block.pprof
```

### 监控指标
1. **QPS**：每秒请求数
2. **延迟分布**：P50, P90, P99, P999
3. **内存分配**：每秒分配次数和大小
4. **GC停顿**：GC次数和停顿时间
5. **CPU使用**：用户态/内核态时间比例

## 风险控制

### 技术风险
1. **兼容性风险**：自定义网络栈可能不兼容某些HTTP特性
2. **稳定性风险**：激进优化可能引入bug
3. **维护成本**：优化代码可能更难维护

### 缓解措施
1. **渐进实施**：分阶段优化，每阶段充分测试
2. **A/B测试**：保留原始实现作为fallback
3. **全面测试**：功能测试、性能测试、压力测试

## 结论

go-wrk的性能瓶颈主要来自Go运行时开销和标准库抽象。通过深度优化，有望将性能从11,500 QPS提升到23,000-25,000 QPS，接近wrk的水平。

### 关键成功因素
1. **编译优化**：减少运行时开销
2. **网络栈优化**：减少抽象层
3. **内存管理**：减少GC压力
4. **并发优化**：提高CPU利用率

### 建议
1. **立即开始**：实施阶段1的编译优化
2. **数据驱动**：基于profiling结果优化热点
3. **持续迭代**：优化是一个持续过程

通过系统性的深度优化，go-wrk有望达到与wrk相近的性能水平。
