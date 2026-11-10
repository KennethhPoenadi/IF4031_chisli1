package shardstore

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

type StaticNodes map[string]string

func (s StaticNodes) NodeOf(_ context.Context, unit string) (string, error) {
	if n, ok := s[unit]; ok {
		return n, nil
	}
	return "", fmt.Errorf("node untuk unit %q tidak diketahui", unit)
}

func ParseStaticNodes(spec string) (StaticNodes, error) {
	out := StaticNodes{}
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		k, v, ok := strings.Cut(part, "=")
		if !ok || strings.TrimSpace(k) == "" || strings.TrimSpace(v) == "" {
			return nil, fmt.Errorf("pemetaan node %q tidak valid, formatnya unit=node", part)
		}
		out[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}
	return out, nil
}

const serviceAccountDir = "/var/run/secrets/kubernetes.io/serviceaccount"

type KubeNodes struct {
	base      string
	namespace string
	token     func() (string, error)
	client    *http.Client
	ttl       time.Duration

	mu    sync.Mutex
	cache map[string]cachedNode
}

type cachedNode struct {
	node    string
	expires time.Time
}

func NewKubeNodes() (*KubeNodes, error) {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, fmt.Errorf("tidak berjalan di dalam cluster Kubernetes")
	}
	ns, err := os.ReadFile(serviceAccountDir + "/namespace")
	if err != nil {
		return nil, err
	}
	ca, err := os.ReadFile(serviceAccountDir + "/ca.crt")
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca)
	return &KubeNodes{
		base:      "https://" + host + ":" + port,
		namespace: strings.TrimSpace(string(ns)),
		// token dibaca ulang tiap permintaan karena Kubernetes merotasinya
		token: func() (string, error) {
			b, err := os.ReadFile(serviceAccountDir + "/token")
			return strings.TrimSpace(string(b)), err
		},
		client: &http.Client{Timeout: 2 * time.Second,
			Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}},
		ttl:   30 * time.Second,
		cache: map[string]cachedNode{},
	}, nil
}

func (k *KubeNodes) NodeOf(ctx context.Context, unit string) (string, error) {
	k.mu.Lock()
	if c, ok := k.cache[unit]; ok && time.Now().Before(c.expires) {
		k.mu.Unlock()
		return c.node, nil
	}
	k.mu.Unlock()

	token, err := k.token()
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		k.base+"/api/v1/namespaces/"+url.PathEscape(k.namespace)+"/pods/"+url.PathEscape(unit), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := k.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("API Kubernetes menjawab %d untuk pod %s", resp.StatusCode, unit)
	}
	var pod struct {
		Spec struct {
			NodeName string `json:"nodeName"`
		} `json:"spec"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&pod); err != nil {
		return "", err
	}
	if pod.Spec.NodeName == "" {
		return "", fmt.Errorf("pod %s belum dijadwalkan", unit)
	}
	k.mu.Lock()
	k.cache[unit] = cachedNode{node: pod.Spec.NodeName, expires: time.Now().Add(k.ttl)}
	k.mu.Unlock()
	return pod.Spec.NodeName, nil
}

type FallbackNodes []NodeResolver

func (f FallbackNodes) NodeOf(ctx context.Context, unit string) (string, error) {
	var last error
	for _, r := range f {
		n, err := r.NodeOf(ctx, unit)
		if err == nil {
			return n, nil
		}
		last = err
	}
	if last == nil {
		last = fmt.Errorf("tidak ada resolver node")
	}
	return "", last
}
