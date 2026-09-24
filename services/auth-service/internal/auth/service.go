package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"auth-service/internal/config"
)

var (
	ErrInvalidClient = errors.New("invalid client credentials")
	ErrInvalidGrant  = errors.New("invalid, expired or revoked refresh token")
	ErrTokenReuse = errors.New("refresh token reuse detected, session revoked")
)

type Claims struct {
	Scope     string `json:"scope"`
	SessionID string `json:"sid"`
	jwt.RegisteredClaims
}

type TokenPair struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
	RefreshToken string `json:"refresh_token"`
	Scope        string `json:"scope"`
	ClientID     string `json:"-"` // for logging only, never serialized
}

type Revoked struct {
	JTI string `json:"jti"`
	Exp int64  `json:"exp"`
}

type clientRec struct {
	id         string
	secretHash [32]byte
	scope      string
}

type session struct {
	clientID string
	scope    string
	jti      string    // jti of the newest access token of this session
	jtiExp   time.Time 
	expires  time.Time // expiry of the newest refresh token
	revoked  bool
}

type refreshRec struct {
	sid  string
	exp  time.Time
	used bool
}

type Service struct {
	cfg     config.Config
	clients map[string]clientRec
	now     func() time.Time

	mu       sync.Mutex
	sessions map[string]*session
	refresh  map[[32]byte]*refreshRec // keyed by SHA-256 of the token: raw refresh tokens are never stored
	revoked  map[string]time.Time     // jti -> exp of the access token
}

func NewService(cfg config.Config) *Service {
	s := &Service{
		cfg:      cfg,
		clients:  make(map[string]clientRec, len(cfg.Clients)),
		now:      time.Now,
		sessions: map[string]*session{},
		refresh:  map[[32]byte]*refreshRec{},
		revoked:  map[string]time.Time{},
	}
	for _, c := range cfg.Clients {
		s.clients[c.ID] = clientRec{id: c.ID, secretHash: sha256.Sum256([]byte(c.Secret)), scope: c.Scope}
	}
	return s
}

func (s *Service) PublicKey() ed25519.PublicKey {
	return s.cfg.SigningKey.Public().(ed25519.PublicKey)
}

func (s *Service) Login(clientID, secret string) (TokenPair, error) {
	rec, known := s.clients[clientID]
	got := sha256.Sum256([]byte(secret))
	want := rec.secretHash 
	ok := subtle.ConstantTimeCompare(got[:], want[:]) == 1
	if !known || !ok {
		return TokenPair{}, ErrInvalidClient
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	sid := randomToken(16)
	sess := &session{clientID: rec.id, scope: rec.scope}
	s.sessions[sid] = sess
	return s.issueLocked(sid, sess)
}

func (s *Service) Refresh(refreshToken string) (TokenPair, error) {
	key := sha256.Sum256([]byte(refreshToken))

	s.mu.Lock()
	defer s.mu.Unlock()

	rec, ok := s.refresh[key]
	if !ok {
		return TokenPair{}, ErrInvalidGrant
	}
	sess, ok := s.sessions[rec.sid]
	if !ok || sess.revoked {
		return TokenPair{}, ErrInvalidGrant
	}
	if rec.used {
		s.revokeSessionLocked(sess)
		return TokenPair{}, ErrTokenReuse
	}
	if s.now().After(rec.exp) {
		return TokenPair{}, ErrInvalidGrant
	}

	rec.used = true
	s.revokeJTILocked(sess.jti, sess.jtiExp)
	return s.issueLocked(rec.sid, sess)
}

func (s *Service) Revocations() []Revoked {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	out := make([]Revoked, 0, len(s.revoked))
	for jti, exp := range s.revoked {
		if now.After(exp) {
			delete(s.revoked, jti)
			continue
		}
		out = append(out, Revoked{JTI: jti, Exp: exp.Unix()})
	}
	return out
}

func (s *Service) Sweep() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	for k, r := range s.refresh {
		if now.After(r.exp) {
			delete(s.refresh, k)
		}
	}
	for id, sess := range s.sessions {
		if now.After(sess.expires) {
			delete(s.sessions, id)
		}
	}
}

func (s *Service) issueLocked(sid string, sess *session) (TokenPair, error) {
	now := s.now()
	jti := randomToken(16)
	exp := now.Add(s.cfg.AccessTTL)

	claims := Claims{
		Scope:     sess.scope,
		SessionID: sid,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    s.cfg.Issuer,
			Subject:   sess.clientID,
			ID:        jti,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(exp),
		},
	}
	signed, err := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims).SignedString(s.cfg.SigningKey)
	if err != nil {
		return TokenPair{}, err
	}

	rt := randomToken(32)
	refreshExp := now.Add(s.cfg.RefreshTTL)
	s.refresh[sha256.Sum256([]byte(rt))] = &refreshRec{sid: sid, exp: refreshExp}

	sess.jti, sess.jtiExp, sess.expires = jti, exp, refreshExp
	return TokenPair{
		AccessToken:  signed,
		TokenType:    "Bearer",
		ExpiresIn:    int(s.cfg.AccessTTL.Seconds()),
		RefreshToken: rt,
		Scope:        sess.scope,
		ClientID:     sess.clientID,
	}, nil
}

func (s *Service) revokeSessionLocked(sess *session) {
	sess.revoked = true
	s.revokeJTILocked(sess.jti, sess.jtiExp)
}

func (s *Service) revokeJTILocked(jti string, exp time.Time) {
	if jti != "" && exp.After(s.now()) {
		s.revoked[jti] = exp
	}
}

func randomToken(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return base64.RawURLEncoding.EncodeToString(b)
}
