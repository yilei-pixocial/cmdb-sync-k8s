package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/olebedev/config"
	cmdbsdk "github.com/veops/cmdb-sdk-golang"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Config 服务配置
type Config struct {
	RedisAddr        string        `json:"redisAddr"`
	RedisPassword    string        `json:"redisPassword"`
	ClusterKeyPrefix string        `json:"clusterKeyPrefix"`
	SyncTimeout      time.Duration `json:"syncTimeout"`
	DB               int           `json:"db"`
	CmdbUrl          string        `json:"cmdbUrl"`
	CmdbApiKey       string        `json:"cmdbApiKey"`
	CmdbApiSecret    string        `json:"cmdbApiSecret"`
}

// SyncService 同步服务
type SyncService struct {
	redisClient *redis.Client
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	config      Config

	// 本地状态
	localCache     map[string]string // 记录已同步的key-value
	localCacheLock sync.RWMutex      // 本地缓存锁

	// 监控指标
	metrics struct {
		syncCount  int64
		errorCount int64
	}
}

func NewSyncService(cfg Config) *SyncService {
	ctx, cancel := context.WithCancel(context.Background())
	return &SyncService{
		redisClient: redis.NewClient(&redis.Options{
			Addr:     cfg.RedisAddr,
			Password: cfg.RedisPassword,
			DB:       cfg.DB,
		}),
		ctx:        ctx,
		cancel:     cancel,
		config:     cfg,
		localCache: make(map[string]string),
	}
}

func (s *SyncService) Start() error {
	log.Println("Starting sync service...")

	if err := s.redisClient.Ping(s.ctx).Err(); err != nil {
		return fmt.Errorf("failed to connect to Redis: %v", err)
	}

	// 启用Keyspace通知：订阅过期事件 (x) 和 通用的写事件 (g)，特别是针对字符串的修改 ($)
	if err := s.redisClient.ConfigSet(s.ctx, "notify-keyspace-events", "Exg$").Err(); err != nil {
		return fmt.Errorf("failed to set keyspace notifications: %v", err)
	}

	// 启动全局key监听
	s.wg.Add(1)
	go s.watchKeyEvents()

	log.Println("Sync service started")
	return nil
}

// Stop 停止服务
func (s *SyncService) Stop() {
	log.Println("Shutting down sync service...")

	// 取消上下文
	s.cancel()

	// 等待所有goroutine退出
	s.wg.Wait()

	// 关闭Redis连接
	if err := s.redisClient.Close(); err != nil {
		log.Printf("Error closing Redis client: %v", err)
	}

	log.Println("Sync service stopped")
}

// watchKeyEvents 监听全局key变更事件
func (s *SyncService) watchKeyEvents() {
	defer s.wg.Done()

	// 订阅redis中set事件
	pattern := fmt.Sprintf("__key*@%d__:set", s.config.DB)
	log.Printf("[Global] Starting key watcher with pattern: %s", pattern)

	retryCount := 0
	maxRetries := 3

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
			pubsub := s.redisClient.PSubscribe(s.ctx, pattern)
			ch := pubsub.Channel()

		listenLoop:
			for {
				select {
				case <-s.ctx.Done():
					pubsub.Close()
					return
				case msg, ok := <-ch:
					if !ok {
						pubsub.Close()
						break listenLoop
					}

					log.Printf("[Event] Received key change: %s", msg.Payload)

					// 检查是否是我们关心的集群key
					if strings.HasPrefix(msg.Payload, s.config.ClusterKeyPrefix) {
						s.handleKeyChange(msg.Payload)
					} else {
						log.Printf("[Event] Ignoring non-cluster key: %s", msg.Payload)
					}
				}
			}

			retryCount++
			if retryCount > maxRetries {
				log.Printf("[Watcher] Max retries reached, giving up")
				return
			}

			waitTime := time.Duration(retryCount) * time.Second
			log.Printf("[Watcher] Connection lost, retrying in %v (attempt %d/%d)",
				waitTime, retryCount, maxRetries)
			time.Sleep(waitTime)
		}
	}
}

// handleKeyChange 处理key变更
func (s *SyncService) handleKeyChange(key string) {
	ctx, cancel := context.WithTimeout(s.ctx, s.config.SyncTimeout)
	defer cancel()

	// 获取当前值
	value, err := s.redisClient.Get(ctx, key).Result()
	if err != nil {
		if err != redis.Nil {
			log.Printf("[Sync] Error getting key %s: %v", key, err)
			s.metrics.errorCount++
		}
		return
	}

	// 检查是否需要同步
	s.localCacheLock.RLock()
	lastValue, exists := s.localCache[key]
	s.localCacheLock.RUnlock()

	if exists && strings.Compare(lastValue, value) == 0 {
		log.Printf("[Sync] Key %s already up to date, skipping", key)
		return
	}

	// 执行同步
	if err := s.syncToCMDB(ctx, key, value); err != nil {
		log.Printf("[Sync] Error syncing key %s: %v", key, err)
		s.metrics.errorCount++
		return
	}

	// 更新本地缓存
	s.localCacheLock.Lock()
	s.localCache[key] = value
	s.localCacheLock.Unlock()

	s.metrics.syncCount++
	log.Printf("[Sync] Successfully synced key: %s", key)
}

type Cluster struct {
	ClusterID       string      `json:"clusterID"`
	ClusterName     string      `json:"clusterName"`
	Namespace       []Namespace `json:"namespaces"`
	Services        []service   `json:"services"`
	K8sVersion      string      `json:"k8sVersion"`
	ClusterRegionID string      `json:"clusterRegionID"`
}

type Namespace struct {
	NamespaceID string `json:"namespaceID"`
	Name        string `json:"name"`
	CreatedTime string `json:"createdTime"`
	ClusterID   string `json:"clusterID"`
	ClusterName string `json:"clusterName"`
	Status      string `json:"status"`
}

type service struct {
	ServiceID   string      `json:"serviceID"`
	Name        string      `json:"name"`
	NamespaceID string      `json:"namespaceID"`
	Namespace   string      `json:"namespace"`
	ClusterIP   string      `json:"clusterIP"`
	ClusterID   string      `json:"clusterID"`
	Ports       interface{} `json:"ports"`
	CreatedTime string      `json:"createdTime"`
	ClusterName string      `json:"clusterName"`
	Selector    interface{} `json:"selector"`
	Type        string      `json:"type"`
}

type CIOperation struct {
	CIType    string         `json:"CIType"`
	CIKey     string         `json:"CIKey"` // 唯一标识符
	CIID      float64        `json:"CIID"`
	Attrs     map[string]any `json:"attrs"`
	Operation string         `json:"operation"` // "add", "update", "delete"
}

func (s *SyncService) syncToCMDB(ctx context.Context, key, value string) error {

	cmdbURL := s.config.CmdbUrl
	cmdbApiKey := s.config.CmdbApiKey
	cmdbApiSecret := s.config.CmdbApiSecret

	if cmdbURL == "" || cmdbApiKey == "" || cmdbApiSecret == "" {
		return fmt.Errorf("invalid CMDB configuration")
	}

	var newClusterData Cluster
	if err := json.Unmarshal([]byte(value), &newClusterData); err != nil {
		log.Fatalf("Error unmarshalling Redis data: %v", err)
	}

	cmdbHelper := cmdbsdk.NewHelper(cmdbURL, cmdbApiKey, cmdbApiSecret)

	// 获取当前运行的数据
	clusterData := getCIRes(cmdbHelper)

	var needUpdateData []CIOperation
	var needAddData []CIOperation
	var needDeleteData []CIOperation

	oldDataMap := make(map[string]CIOperation)
	for _, old := range clusterData {
		key := old.CIType + ":" + old.CIKey
		oldDataMap[key] = old
	}

	key = "cluster:" + newClusterData.ClusterID
	if old, exists := oldDataMap[key]; exists {
		needUpdateData = append(needUpdateData, CIOperation{
			CIType: "cluster",
			CIKey:  newClusterData.ClusterID,
			CIID:   old.CIID,
			Attrs: map[string]any{
				"ClusterId":       newClusterData.ClusterID,
				"clusterName":     newClusterData.ClusterName,
				"version":         newClusterData.K8sVersion,
				"clusterRegionID": newClusterData.ClusterRegionID,
			},
			Operation: "update",
		})
		delete(oldDataMap, key) // 从map中移除，剩下的就是需要删除的
	} else {
		needAddData = append(needAddData, CIOperation{
			CIType: "cluster",
			CIKey:  newClusterData.ClusterID,
			Attrs: map[string]any{
				"ClusterId":       newClusterData.ClusterID,
				"clusterName":     newClusterData.ClusterName,
				"version":         newClusterData.K8sVersion,
				"clusterRegionID": newClusterData.ClusterRegionID,
			},
			Operation: "add",
		})
	}

	for _, namespace := range newClusterData.Namespace {
		key = "namespace:" + namespace.NamespaceID
		if old, exists := oldDataMap[key]; exists {
			// 只要CIType和CIKey匹配就认为是需要更新
			needUpdateData = append(needUpdateData, CIOperation{
				CIType: "namespace",
				CIKey:  namespace.NamespaceID,
				CIID:   old.CIID,
				Attrs: map[string]any{
					"namespaceID":   namespace.NamespaceID,
					"namespaceName": namespace.Name,
					"createTimeStr": namespace.CreatedTime,
					"ClusterId":     namespace.ClusterID,
					"clusterName":   namespace.ClusterName,
					"status":        namespace.Status,
				},
				Operation: "update",
			})
			delete(oldDataMap, key) // 从map中移除，剩下的就是需要删除的
		} else {
			needAddData = append(needAddData, CIOperation{
				CIType: "namespace",
				CIKey:  namespace.NamespaceID,
				Attrs: map[string]any{
					"namespaceID":   namespace.NamespaceID,
					"namespaceName": namespace.Name,
					"createTimeStr": namespace.CreatedTime,
					"ClusterId":     namespace.ClusterID,
					"clusterName":   namespace.ClusterName,
					"status":        namespace.Status,
				},
				Operation: "add",
			})
		}
	}

	for _, svc := range newClusterData.Services {
		key = "service:" + svc.ServiceID
		if old, exists := oldDataMap[key]; exists {
			needUpdateData = append(needUpdateData, CIOperation{
				CIType: "service",
				CIKey:  svc.ServiceID,
				CIID:   old.CIID,
				Attrs: map[string]any{
					"serviceID":     svc.ServiceID,
					"serviceName":   svc.Name,
					"namespaceID":   svc.NamespaceID,
					"namespaceName": svc.Namespace,
					"clusterIP":     svc.ClusterIP,
					"ports":         svc.Ports,
					"ClusterId":     svc.ClusterID,
					"clusterName":   svc.ClusterName,
					"selector":      svc.Selector,
					"type":          svc.Type,
					"createTimeStr": svc.CreatedTime,
				},
				Operation: "update",
			})
			delete(oldDataMap, key)
		} else {
			needAddData = append(needAddData, CIOperation{
				CIType: "service",
				CIKey:  svc.ServiceID,
				Attrs: map[string]any{
					"serviceID":     svc.ServiceID,
					"serviceName":   svc.Name,
					"namespaceID":   svc.NamespaceID,
					"namespaceName": svc.Namespace,
					"clusterIP":     svc.ClusterIP,
					"ports":         svc.Ports,
					"ClusterId":     svc.ClusterID,
					"clusterName":   svc.ClusterName,
					"selector":      svc.Selector,
					"type":          svc.Type,
					"createTimeStr": svc.CreatedTime,
				},
				Operation: "add",
			})
		}
	}

	// 剩余在oldDataMap中的就是需要删除的
	for _, old := range oldDataMap {
		needDeleteData = append(needDeleteData, CIOperation{
			CIType:    old.CIType,
			CIKey:     old.CIKey,
			CIID:      old.CIID,
			Operation: "delete",
		})
	}

	log.Printf("开始同步集群数据: %s", newClusterData.ClusterID)

	addCI(cmdbHelper, needAddData)
	updateCI(cmdbHelper, needUpdateData)
	deleteCI(cmdbHelper, needDeleteData)

	// 记录同步日志
	return nil
}

func addCI(helper *cmdbsdk.Helper, ciOperations []CIOperation) {
	for _, operation := range ciOperations {
		_, err := helper.AddCI(operation.CIType, cmdbsdk.ExistPolicyReplace, cmdbsdk.ExistPolicyReplace, operation.Attrs)
		if err != nil {
			log.Printf("新增资产失败: %v", err)
		} else {
			log.Printf("新增 %s 成功, : %s", operation.CIType, operation.CIKey)
		}
	}
}

func deleteCI(helper *cmdbsdk.Helper, ciOperations []CIOperation) {
	for _, operation := range ciOperations {
		_, err := helper.DeleteCI(int(operation.CIID))
		if err != nil {
			log.Printf("删除资产失败: %v", err)
		} else {
			log.Printf("删除 %s 成功, : %s", operation.CIType, operation.CIKey)
		}
	}
}

func updateCI(helper *cmdbsdk.Helper, ciOperations []CIOperation) {
	for _, operation := range ciOperations {
		_, err := helper.UpdateCI(int(operation.CIID), cmdbsdk.ExistPolicyReplace, cmdbsdk.ExistPolicyReplace, operation.Attrs)
		if err != nil {
			log.Printf("更新资产失败: %v", err)
		} else {
			log.Printf("更新 %s 成功, : %s", operation.CIType, operation.CIKey)
		}
	}
}

func getCIRes(helper *cmdbsdk.Helper) []CIOperation {

	var ciData []CIOperation
	clusterRes, err := helper.GetCI(fmt.Sprintf("_type:cluster"), "", "", "", 0, 0, cmdbsdk.RetKeyDefault)
	if err != nil {
		log.Printf("  -> 查询资产失败: %v", err)
	} else {
		for _, rMap := range clusterRes.Result {
			ciData = append(ciData, CIOperation{
				CIType: "cluster",
				CIKey:  rMap["ClusterId"].(string),
				CIID:   rMap["_id"].(float64),
				Attrs:  nil,
			})
		}
	}

	namespaceRes, err := helper.GetCI(fmt.Sprintf("_type:namespace"), "", "", "", 0, 0, cmdbsdk.RetKeyDefault)
	if err != nil {
		log.Printf("  -> 查询资产失败: %v", err)
	} else {
		for _, rMap := range namespaceRes.Result {
			ciData = append(ciData, CIOperation{
				CIType: "namespace",
				CIKey:  rMap["namespaceID"].(string),
				CIID:   rMap["_id"].(float64),
				Attrs:  nil,
			})
		}
	}

	serviceRes, err := helper.GetCI(fmt.Sprintf("_type:service"), "", "", "", 0, 0, cmdbsdk.RetKeyDefault)
	if err != nil {
		log.Printf("  -> 查询资产失败: %v", err)
	} else {
		for _, rMap := range serviceRes.Result {
			ciData = append(ciData, CIOperation{
				CIType: "service",
				CIKey:  rMap["serviceID"].(string),
				CIID:   rMap["_id"].(float64),
				Attrs:  nil,
			})
		}
	}

	return ciData
}

func main() {

	pwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("Failed to get working directory: %v", err)
		return
	}

	configPath := filepath.Join(pwd, "configs", "application.yml")
	serverCfg, err := config.ParseYamlFile(configPath)
	if err != nil {
		log.Fatalf("Failed to parse config: %v", err)
		return
	}

	cfg := Config{
		RedisAddr:        serverCfg.UString("redis.addr"),
		RedisPassword:    serverCfg.UString("redis.password"),
		ClusterKeyPrefix: serverCfg.UString("redis.clusterKeyPrefix"),
		DB:               serverCfg.UInt("redis.db"),
		SyncTimeout:      5 * time.Second,
		CmdbUrl:          serverCfg.UString("cmdb.apiUrl"),
		CmdbApiKey:       serverCfg.UString("cmdb.apiKey"),
		CmdbApiSecret:    serverCfg.UString("cmdb.apiSecret"),
	}

	service := NewSyncService(cfg)

	if err := service.Start(); err != nil {
		log.Fatalf("Failed to start service: %v", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	log.Println("Received signal, stopping service...")

	service.Stop()
}
