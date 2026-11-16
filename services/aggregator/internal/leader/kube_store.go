package leader

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	serviceAccountDir = "/var/run/secrets/kubernetes.io/serviceaccount"
	maxLeaseResponse  = 1 << 20
)

type KubeStoreConfig struct {
	BaseURL   string
	Namespace string
	Name      string
	Token     func() (string, error)
	Client    *http.Client
}

type KubeStore struct {
	baseURL   string
	namespace string
	name      string
	token     func() (string, error)
	client    *http.Client
}

func NewKubeStore(cfg KubeStoreConfig) (*KubeStore, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	parsed, err := url.Parse(baseURL)
	switch {
	case err != nil || parsed.Scheme == "" || parsed.Host == "":
		return nil, errors.New("kubernetes API base URL is invalid")
	case parsed.Scheme != "http" && parsed.Scheme != "https":
		return nil, errors.New("kubernetes API base URL must use HTTP or HTTPS")
	case strings.TrimSpace(cfg.Namespace) == "":
		return nil, errors.New("leader lease namespace must not be blank")
	case strings.TrimSpace(cfg.Name) == "":
		return nil, errors.New("leader lease name must not be blank")
	case cfg.Token == nil:
		return nil, errors.New("kubernetes service account token source is required")
	case cfg.Client == nil:
		return nil, errors.New("Kubernetes HTTP client is required")
	}
	return &KubeStore{
		baseURL: baseURL, namespace: strings.TrimSpace(cfg.Namespace),
		name: strings.TrimSpace(cfg.Name), token: cfg.Token, client: cfg.Client,
	}, nil
}

func NewInClusterStore(name string) (*KubeStore, error) {
	host := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_HOST"))
	port := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_PORT_HTTPS"))
	if port == "" {
		port = strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_PORT"))
	}
	if host == "" || port == "" {
		return nil, errors.New("Kubernetes in-cluster API environment is unavailable")
	}

	namespace, err := os.ReadFile(serviceAccountDir + "/namespace")
	if err != nil {
		return nil, fmt.Errorf("read service account namespace: %w", err)
	}
	ca, err := os.ReadFile(serviceAccountDir + "/ca.crt")
	if err != nil {
		return nil, fmt.Errorf("read Kubernetes CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New("Kubernetes CA bundle contains no certificates")
	}

	return NewKubeStore(KubeStoreConfig{
		BaseURL:   "https://" + net.JoinHostPort(host, port),
		Namespace: strings.TrimSpace(string(namespace)), Name: name,
		Token: func() (string, error) {
			raw, readErr := os.ReadFile(serviceAccountDir + "/token")
			if readErr != nil {
				return "", readErr
			}
			token := strings.TrimSpace(string(raw))
			if token == "" {
				return "", errors.New("service account token is blank")
			}
			return token, nil
		},
		Client: &http.Client{
			Timeout: 2 * time.Second,
			Transport: &http.Transport{TLSClientConfig: &tls.Config{
				RootCAs: pool, MinVersion: tls.VersionTLS12,
			}},
		},
	})
}

func (k *KubeStore) Get(ctx context.Context) (Lease, time.Time, error) {
	response, serverNow, err := k.request(ctx, http.MethodGet, k.itemURL(), nil)
	if err != nil {
		return Lease{}, serverNow, err
	}
	return decodeLease(response, serverNow)
}

func (k *KubeStore) Create(ctx context.Context, desired Lease) (Lease, time.Time, error) {
	body, err := encodeLease(k.namespace, k.name, desired)
	if err != nil {
		return Lease{}, time.Time{}, err
	}
	response, serverNow, err := k.request(ctx, http.MethodPost, k.collectionURL(), body)
	if err != nil {
		return Lease{}, serverNow, err
	}
	return decodeLease(response, serverNow)
}

func (k *KubeStore) Update(ctx context.Context, current, desired Lease) (Lease, time.Time, error) {
	if current.ResourceVersion == "" {
		return Lease{}, time.Time{}, errors.New("leader lease resourceVersion is required for update")
	}
	desired.ResourceVersion = current.ResourceVersion
	desired.UID = current.UID
	body, err := encodeLease(k.namespace, k.name, desired)
	if err != nil {
		return Lease{}, time.Time{}, err
	}
	response, serverNow, err := k.request(ctx, http.MethodPut, k.itemURL(), body)
	if err != nil {
		return Lease{}, serverNow, err
	}
	return decodeLease(response, serverNow)
}

func (k *KubeStore) collectionURL() string {
	return k.baseURL + "/apis/coordination.k8s.io/v1/namespaces/" +
		url.PathEscape(k.namespace) + "/leases"
}

func (k *KubeStore) itemURL() string {
	return k.collectionURL() + "/" + url.PathEscape(k.name)
}

func (k *KubeStore) request(ctx context.Context, method, endpoint string, body []byte) ([]byte, time.Time, error) {
	token, err := k.token()
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("read service account token: %w", err)
	}
	if strings.TrimSpace(token) == "" {
		return nil, time.Time{}, errors.New("service account token is blank")
	}

	request, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, time.Time{}, err
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}

	response, err := k.client.Do(request)
	if err != nil {
		return nil, time.Time{}, err
	}
	defer response.Body.Close()
	serverNow, err := http.ParseTime(response.Header.Get("Date"))
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("Kubernetes API Date header is invalid: %w", err)
	}
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, maxLeaseResponse+1))
	if err != nil {
		return nil, serverNow, err
	}
	if len(responseBody) > maxLeaseResponse {
		return nil, serverNow, errors.New("Kubernetes Lease response exceeds size limit")
	}

	switch response.StatusCode {
	case http.StatusOK, http.StatusCreated:
		return responseBody, serverNow, nil
	case http.StatusNotFound:
		return nil, serverNow, ErrLeaseNotFound
	case http.StatusConflict:
		return nil, serverNow, ErrLeaseConflict
	default:
		return nil, serverNow, fmt.Errorf("Kubernetes Lease API returned HTTP %d", response.StatusCode)
	}
}

type kubeLease struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		Name            string `json:"name"`
		Namespace       string `json:"namespace,omitempty"`
		UID             string `json:"uid,omitempty"`
		ResourceVersion string `json:"resourceVersion,omitempty"`
	} `json:"metadata"`
	Spec struct {
		HolderIdentity       string `json:"holderIdentity"`
		LeaseDurationSeconds int64  `json:"leaseDurationSeconds"`
		AcquireTime          string `json:"acquireTime,omitempty"`
		RenewTime            string `json:"renewTime"`
		LeaseTransitions     int64  `json:"leaseTransitions"`
	} `json:"spec"`
}

func encodeLease(namespace, name string, lease Lease) ([]byte, error) {
	if lease.Holder == "" || lease.Transitions < 1 || lease.RenewedAt.IsZero() || lease.Duration <= 0 {
		return nil, errors.New("leader lease is incomplete")
	}
	durationSeconds := int64(math.Ceil(lease.Duration.Seconds()))
	if durationSeconds < 1 {
		return nil, errors.New("leader lease duration is too short")
	}
	if durationSeconds > 1<<31-1 {
		return nil, errors.New("leader lease duration exceeds Kubernetes limit")
	}

	resource := kubeLease{APIVersion: "coordination.k8s.io/v1", Kind: "Lease"}
	resource.Metadata.Name = name
	resource.Metadata.Namespace = namespace
	resource.Metadata.UID = lease.UID
	resource.Metadata.ResourceVersion = lease.ResourceVersion
	resource.Spec.HolderIdentity = lease.Holder
	resource.Spec.LeaseDurationSeconds = durationSeconds
	resource.Spec.AcquireTime = formatMicroTime(lease.AcquiredAt)
	resource.Spec.RenewTime = formatMicroTime(lease.RenewedAt)
	resource.Spec.LeaseTransitions = lease.Transitions
	return json.Marshal(resource)
}

func decodeLease(raw []byte, serverNow time.Time) (Lease, time.Time, error) {
	var resource kubeLease
	if err := json.Unmarshal(raw, &resource); err != nil {
		return Lease{}, serverNow, fmt.Errorf("decode Kubernetes Lease: %w", err)
	}
	renewedAt, err := time.Parse(time.RFC3339Nano, resource.Spec.RenewTime)
	if err != nil {
		return Lease{}, serverNow, fmt.Errorf("decode Kubernetes Lease renewTime: %w", err)
	}
	var acquiredAt time.Time
	if resource.Spec.AcquireTime != "" {
		acquiredAt, err = time.Parse(time.RFC3339Nano, resource.Spec.AcquireTime)
		if err != nil {
			return Lease{}, serverNow, fmt.Errorf("decode Kubernetes Lease acquireTime: %w", err)
		}
	}
	lease := Lease{
		ResourceVersion: resource.Metadata.ResourceVersion,
		UID:             resource.Metadata.UID,
		Holder:          resource.Spec.HolderIdentity,
		Transitions:     resource.Spec.LeaseTransitions,
		AcquiredAt:      acquiredAt,
		RenewedAt:       renewedAt,
		Duration:        time.Duration(resource.Spec.LeaseDurationSeconds) * time.Second,
	}
	if lease.ResourceVersion == "" || lease.UID == "" || lease.Holder == "" || lease.Duration <= 0 {
		return Lease{}, serverNow, errors.New("Kubernetes Lease response is incomplete")
	}
	return lease, serverNow, nil
}

func formatMicroTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}
