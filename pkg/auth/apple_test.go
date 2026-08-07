package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestAppleVerifiesMapsAndCaches(t *testing.T) {
	privateKey := generateRSAKey(t)
	var requests atomic.Int32
	server := newJWKSServer(t, func() ([]byte, string) {
		requests.Add(1)
		return jwksDocument(t, "key-1", &privateKey.PublicKey), "public, max-age=3600"
	})
	defer server.Close()
	verifier := newAppleTestVerifier(t, server, "com.example.app")
	now := time.Unix(2_000_000_000, 0)
	verifier.now = func() time.Time { return now }
	token := signAppleToken(t, privateKey, "key-1", jwt.MapClaims{
		"iss": appleIssuer, "sub": "apple-user", "aud": "com.example.app",
		"iat": now.Add(-time.Minute).Unix(), "exp": now.Add(time.Hour).Unix(),
		"nonce": "expected", "email": "relay@privaterelay.appleid.com", "email_verified": "true",
	})
	nonce := "expected"
	for range 2 {
		identity, err := verifier.Verify(context.Background(), Credential{Token: token, ExpectedNonce: &nonce})
		if err != nil {
			t.Fatal(err)
		}
		if identity.Provider != ProviderApple || identity.LoginID != "apple-user" ||
			identity.Email == nil || *identity.Email != "relay@privaterelay.appleid.com" ||
			identity.EmailVerified == nil || !*identity.EmailVerified || identity.Username != nil {
			t.Fatalf("unexpected identity: %#v", identity)
		}
	}
	if requests.Load() != 1 {
		t.Fatalf("JWKS requests = %d, want 1", requests.Load())
	}
}

func TestAppleRejectsInvalidClaims(t *testing.T) {
	privateKey := generateRSAKey(t)
	server := newJWKSServer(t, func() ([]byte, string) {
		return jwksDocument(t, "key", &privateKey.PublicKey), "max-age=3600"
	})
	defer server.Close()
	verifier := newAppleTestVerifier(t, server, "client-id")
	now := time.Unix(2_000_000_000, 0)
	verifier.now = func() time.Time { return now }

	tests := []struct {
		name   string
		claims jwt.MapClaims
		nonce  *string
		keyID  string
	}{
		{name: "wrong audience", claims: validAppleClaims(now, "other-client")},
		{name: "expired", claims: jwt.MapClaims{"iss": appleIssuer, "sub": "user", "aud": "client-id", "iat": now.Add(-time.Hour).Unix(), "exp": now.Add(-time.Minute).Unix()}},
		{name: "wrong issuer", claims: jwt.MapClaims{"iss": "https://example.com", "sub": "user", "aud": "client-id", "iat": now.Unix(), "exp": now.Add(time.Hour).Unix()}},
		{name: "empty subject", claims: jwt.MapClaims{"iss": appleIssuer, "sub": "", "aud": "client-id", "iat": now.Unix(), "exp": now.Add(time.Hour).Unix()}},
		{name: "unknown key", claims: validAppleClaims(now, "client-id"), keyID: "missing"},
	}
	badNonce := "bad"
	claimsWithNonce := validAppleClaims(now, "client-id")
	claimsWithNonce["nonce"] = "good"
	tests = append(tests, struct {
		name   string
		claims jwt.MapClaims
		nonce  *string
		keyID  string
	}{name: "nonce", claims: claimsWithNonce, nonce: &badNonce})

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			keyID := test.keyID
			if keyID == "" {
				keyID = "key"
			}
			token := signAppleToken(t, privateKey, keyID, test.claims)
			_, err := verifier.Verify(context.Background(), Credential{Token: token, ExpectedNonce: test.nonce})
			if !errors.Is(err, ErrInvalidCredential) {
				t.Fatalf("Verify() error = %v, want ErrInvalidCredential", err)
			}
		})
	}
}

func TestAppleRefreshesUnknownKey(t *testing.T) {
	first := generateRSAKey(t)
	second := generateRSAKey(t)
	var generation atomic.Int32
	var requests atomic.Int32
	server := newJWKSServer(t, func() ([]byte, string) {
		requests.Add(1)
		if generation.Load() == 0 {
			return jwksDocument(t, "first", &first.PublicKey), "max-age=3600"
		}
		return jwksDocument(t, "second", &second.PublicKey), "max-age=3600"
	})
	defer server.Close()
	verifier := newAppleTestVerifier(t, server, "client")
	now := time.Unix(2_000_000_000, 0)
	verifier.now = func() time.Time { return now }

	firstToken := signAppleToken(t, first, "first", validAppleClaims(now, "client"))
	if _, err := verifier.Verify(context.Background(), Credential{Token: firstToken}); err != nil {
		t.Fatal(err)
	}
	generation.Store(1)
	now = now.Add(unknownKeyRefreshMinimum)
	secondToken := signAppleToken(t, second, "second", validAppleClaims(now, "client"))
	if _, err := verifier.Verify(context.Background(), Credential{Token: secondToken}); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 2 {
		t.Fatalf("JWKS requests = %d, want 2", requests.Load())
	}
}

func TestAppleConcurrentVerificationUsesSafeCache(t *testing.T) {
	privateKey := generateRSAKey(t)
	server := newJWKSServer(t, func() ([]byte, string) {
		return jwksDocument(t, "key", &privateKey.PublicKey), "max-age=3600"
	})
	defer server.Close()
	verifier := newAppleTestVerifier(t, server, "client")
	now := time.Unix(2_000_000_000, 0)
	verifier.now = func() time.Time { return now }
	token := signAppleToken(t, privateKey, "key", validAppleClaims(now, "client"))

	var wg sync.WaitGroup
	errorsChannel := make(chan error, 16)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := verifier.Verify(context.Background(), Credential{Token: token})
			errorsChannel <- err
		}()
	}
	wg.Wait()
	close(errorsChannel)
	for err := range errorsChannel {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestAppleClassifiesJWKSFailureAndHidesToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		http.Error(response, "private", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	verifier := newAppleTestVerifier(t, server, "client")
	const token = "secret-token"
	_, err := verifier.Verify(context.Background(), Credential{Token: token})
	// malformed JWT 不会访问 JWKS，因此额外构造一个有 kid 的未签名结构。
	if err == nil || strings.Contains(err.Error(), token) {
		t.Fatalf("malformed error = %v", err)
	}
	privateKey := generateRSAKey(t)
	signed := signAppleToken(t, privateKey, "key", validAppleClaims(time.Now(), "client"))
	_, err = verifier.Verify(context.Background(), Credential{Token: signed})
	if !errors.Is(err, ErrProviderUnavailable) || strings.Contains(err.Error(), signed) {
		t.Fatalf("JWKS error = %v", err)
	}
}

func TestNewAppleValidatesConfig(t *testing.T) {
	for _, config := range []AppleConfig{{}, {ClientIDs: []string{""}}, {ClientIDs: []string{"same", "same"}}} {
		if _, err := NewApple(config); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("NewApple(%+v) error = %v", config, err)
		}
	}
}

func validAppleClaims(now time.Time, audience string) jwt.MapClaims {
	return jwt.MapClaims{
		"iss": appleIssuer,
		"sub": "apple-user",
		"aud": audience,
		"iat": now.Add(-time.Minute).Unix(),
		"exp": now.Add(time.Hour).Unix(),
	}
}

func generateRSAKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func signAppleToken(t *testing.T, key *rsa.PrivateKey, keyID string, claims jwt.MapClaims) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = keyID
	signed, err := token.SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func jwksDocument(t *testing.T, keyID string, key *rsa.PublicKey) []byte {
	t.Helper()
	document := map[string]any{"keys": []map[string]string{{
		"kty": "RSA",
		"kid": keyID,
		"use": "sig",
		"alg": "RS256",
		"n":   base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
		"e":   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
	}}}
	encoded, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func newJWKSServer(t *testing.T, response func() ([]byte, string)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, cacheControl := response()
		writer.Header().Set("Content-Type", "application/json")
		writer.Header().Set("Cache-Control", cacheControl)
		_, _ = writer.Write(body)
	}))
}

func newAppleTestVerifier(t *testing.T, server *httptest.Server, clientID string) *appleVerifier {
	t.Helper()
	created, err := NewApple(AppleConfig{ClientIDs: []string{clientID}, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	verifier := created.(*appleVerifier)
	verifier.jwksURL = server.URL
	return verifier
}

func TestJWKSCacheDuration(t *testing.T) {
	if got := jwksCacheDuration("public, max-age=120"); got != 2*time.Minute {
		t.Fatalf("cache duration = %v", got)
	}
	if got := jwksCacheDuration("no-cache"); got != defaultJWKSCacheTime {
		t.Fatalf("default cache duration = %v", got)
	}
}

func ExampleAuthenticator_Authenticate() {
	// 实际项目中将 verifier 传给 auth.New；这里只展示统一调用形态。
	verifier := &stubVerifier{provider: ProviderGoogle, identity: &Identity{Provider: ProviderGoogle, LoginID: "uid"}}
	authenticator, _ := New(verifier)
	identity, _ := authenticator.Authenticate(context.Background(), Credential{Provider: ProviderGoogle, Token: "id-token"})
	fmt.Println(identity.Provider, identity.LoginID)
	// Output: google uid
}
