package auth

import (
	"context"
	"crypto/rsa"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	appleIssuer              = "https://appleid.apple.com"
	appleJWKSURL             = "https://appleid.apple.com/auth/keys"
	defaultJWKSCacheTime     = time.Hour
	maxJWKSCacheTime         = 24 * time.Hour
	unknownKeyRefreshMinimum = time.Minute
)

// AppleConfig 配置 Sign in with Apple Identity Token 校验器。
type AppleConfig struct {
	ClientIDs  []string
	HTTPClient *http.Client
}

type appleVerifier struct {
	clientIDs  map[string]struct{}
	httpClient *http.Client
	jwksURL    string
	now        func() time.Time

	cacheMu      sync.Mutex
	cachedKeys   map[string]*rsa.PublicKey
	cacheExpires time.Time
	lastRefresh  time.Time
}

// NewApple 创建 Apple Identity Token Verifier。
func NewApple(config AppleConfig) (Verifier, error) {
	if len(config.ClientIDs) == 0 {
		return nil, fmt.Errorf("%w: apple client IDs are empty", ErrInvalidConfig)
	}
	clientIDs := make(map[string]struct{}, len(config.ClientIDs))
	for index, clientID := range config.ClientIDs {
		if strings.TrimSpace(clientID) == "" {
			return nil, fmt.Errorf("%w: apple client ID %d is empty", ErrInvalidConfig, index)
		}
		if _, exists := clientIDs[clientID]; exists {
			return nil, fmt.Errorf("%w: duplicate apple client ID", ErrInvalidConfig)
		}
		clientIDs[clientID] = struct{}{}
	}
	client := config.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: defaultHTTPTimeout}
	}
	return &appleVerifier{
		clientIDs:  clientIDs,
		httpClient: client,
		jwksURL:    appleJWKSURL,
		now:        time.Now,
	}, nil
}

func (v *appleVerifier) Provider() Provider {
	return ProviderApple
}

type appleClaims struct {
	jwt.RegisteredClaims
	Nonce         string      `json:"nonce"`
	Email         string      `json:"email"`
	EmailVerified interface{} `json:"email_verified"`
}

func (v *appleVerifier) Verify(ctx context.Context, credential Credential) (*Identity, error) {
	if err := validateVerifierInput(ctx, credential); err != nil {
		return nil, err
	}
	claims := &appleClaims{}
	parser := jwt.NewParser(
		jwt.WithValidMethods([]string{jwt.SigningMethodRS256.Alg()}),
		jwt.WithIssuer(appleIssuer),
		jwt.WithExpirationRequired(),
		jwt.WithIssuedAt(),
		jwt.WithTimeFunc(v.now),
	)
	token, err := parser.ParseWithClaims(credential.Token, claims, func(token *jwt.Token) (interface{}, error) {
		kid, ok := token.Header["kid"].(string)
		if !ok || kid == "" {
			return nil, fmt.Errorf("%w: apple token has no key ID", ErrInvalidCredential)
		}
		return v.key(ctx, kid)
	})
	if err != nil || token == nil || !token.Valid {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if errors.Is(err, ErrProviderUnavailable) {
			return nil, fmt.Errorf("%w: cannot load Apple signing keys", ErrProviderUnavailable)
		}
		return nil, fmt.Errorf("%w: apple token verification failed", ErrInvalidCredential)
	}
	if strings.TrimSpace(claims.Subject) == "" || claims.IssuedAt == nil || !v.acceptsAudience(claims.Audience) {
		return nil, fmt.Errorf("%w: apple subject or audience is invalid", ErrInvalidCredential)
	}
	if credential.ExpectedNonce != nil && subtle.ConstantTimeCompare([]byte(claims.Nonce), []byte(*credential.ExpectedNonce)) != 1 {
		return nil, fmt.Errorf("%w: apple nonce does not match", ErrInvalidCredential)
	}

	return &Identity{
		Provider:      ProviderApple,
		LoginID:       claims.Subject,
		Email:         stringPointer(claims.Email),
		EmailVerified: appleVerifiedPointer(claims.EmailVerified),
	}, nil
}

func (v *appleVerifier) acceptsAudience(audience jwt.ClaimStrings) bool {
	for _, value := range audience {
		if _, ok := v.clientIDs[value]; ok {
			return true
		}
	}
	return false
}

func (v *appleVerifier) key(ctx context.Context, kid string) (*rsa.PublicKey, error) {
	v.cacheMu.Lock()
	defer v.cacheMu.Unlock()

	now := v.now()
	if now.Before(v.cacheExpires) {
		if key := v.cachedKeys[kid]; key != nil {
			return key, nil
		}
		if now.Sub(v.lastRefresh) < unknownKeyRefreshMinimum {
			return nil, fmt.Errorf("%w: apple key ID is unknown", ErrInvalidCredential)
		}
	}
	// 未知 kid 可能表示 Apple 已轮换密钥，因此即使缓存未过期也刷新一次。
	if err := v.refreshKeys(ctx, now); err != nil {
		return nil, err
	}
	key := v.cachedKeys[kid]
	if key == nil {
		return nil, fmt.Errorf("%w: apple key ID is unknown", ErrInvalidCredential)
	}
	return key, nil
}

type appleJWKS struct {
	Keys []struct {
		KTY string `json:"kty"`
		KID string `json:"kid"`
		Use string `json:"use"`
		Alg string `json:"alg"`
		N   string `json:"n"`
		E   string `json:"e"`
	} `json:"keys"`
}

func (v *appleVerifier) refreshKeys(ctx context.Context, now time.Time) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, v.jwksURL, nil)
	if err != nil {
		return fmt.Errorf("%w: cannot create Apple JWKS request", ErrInvalidConfig)
	}
	response, err := v.httpClient.Do(request)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("%w: Apple JWKS request failed", ErrProviderUnavailable)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxProviderResponse))
		return fmt.Errorf("%w: Apple JWKS returned status %d", ErrProviderUnavailable, response.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(response.Body, maxProviderResponse+1))
	if err != nil || len(body) > maxProviderResponse {
		return fmt.Errorf("%w: Apple JWKS response is too large", ErrProviderUnavailable)
	}
	var document appleJWKS
	if err := json.Unmarshal(body, &document); err != nil {
		return fmt.Errorf("%w: Apple JWKS is invalid", ErrProviderUnavailable)
	}
	keys := make(map[string]*rsa.PublicKey, len(document.Keys))
	for _, encoded := range document.Keys {
		if encoded.KTY != "RSA" || encoded.KID == "" || (encoded.Use != "" && encoded.Use != "sig") ||
			(encoded.Alg != "" && encoded.Alg != jwt.SigningMethodRS256.Alg()) {
			continue
		}
		key, err := decodeRSAKey(encoded.N, encoded.E)
		if err != nil {
			continue
		}
		keys[encoded.KID] = key
	}
	if len(keys) == 0 {
		return fmt.Errorf("%w: Apple JWKS contains no usable key", ErrProviderUnavailable)
	}
	v.cachedKeys = keys
	v.cacheExpires = now.Add(jwksCacheDuration(response.Header.Get("Cache-Control")))
	v.lastRefresh = now
	return nil
}

func decodeRSAKey(modulus, exponent string) (*rsa.PublicKey, error) {
	nBytes, err := base64.RawURLEncoding.DecodeString(modulus)
	if err != nil || len(nBytes) == 0 {
		return nil, fmt.Errorf("invalid modulus")
	}
	eBytes, err := base64.RawURLEncoding.DecodeString(exponent)
	if err != nil || len(eBytes) == 0 {
		return nil, fmt.Errorf("invalid exponent")
	}
	eBig := new(big.Int).SetBytes(eBytes)
	if !eBig.IsInt64() || eBig.Sign() <= 0 || eBig.Int64() > int64(math.MaxInt) {
		return nil, fmt.Errorf("invalid exponent")
	}
	key := &rsa.PublicKey{N: new(big.Int).SetBytes(nBytes), E: int(eBig.Int64())}
	if key.N.BitLen() < 2048 || key.E < 3 {
		return nil, fmt.Errorf("weak RSA key")
	}
	return key, nil
}

func jwksCacheDuration(cacheControl string) time.Duration {
	for _, directive := range strings.Split(cacheControl, ",") {
		name, value, ok := strings.Cut(strings.TrimSpace(directive), "=")
		if !ok || strings.ToLower(name) != "max-age" {
			continue
		}
		seconds, err := strconv.ParseInt(strings.Trim(value, `"`), 10, 64)
		if err == nil && seconds > 0 {
			if seconds >= int64(maxJWKSCacheTime/time.Second) {
				return maxJWKSCacheTime
			}
			return time.Duration(seconds) * time.Second
		}
	}
	return defaultJWKSCacheTime
}

func appleVerifiedPointer(value interface{}) *bool {
	switch typed := value.(type) {
	case bool:
		return boolPointer(typed)
	case string:
		parsed, err := strconv.ParseBool(typed)
		if err == nil {
			return boolPointer(parsed)
		}
	}
	return nil
}
