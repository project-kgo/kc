package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestFacebookVerifiesAndMapsIdentity(t *testing.T) {
	const (
		appID     = "app-123"
		appSecret = "app-secret"
		userToken = "user-token"
	)
	var profileCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/v24.0/debug_token":
			if got := request.Header.Get("Authorization"); got != "Bearer "+appID+"|"+appSecret {
				t.Errorf("debug authorization = %q", got)
			}
			if got := request.URL.Query().Get("input_token"); got != userToken {
				t.Errorf("input_token = %q", got)
			}
			fmt.Fprint(response, `{"data":{"app_id":"app-123","is_valid":true,"user_id":"user-1","expires_at":4102444800}}`)
		case "/v24.0/me":
			profileCalls.Add(1)
			if got := request.Header.Get("Authorization"); got != "Bearer "+userToken {
				t.Errorf("profile authorization = %q", got)
			}
			if got := request.URL.Query().Get("appsecret_proof"); got != facebookAppSecretProof(userToken, appSecret) {
				t.Errorf("appsecret_proof = %q", got)
			}
			fmt.Fprint(response, `{"id":"user-1","name":"Grace","email":"grace@example.com","gender":"female","picture":{"data":{"url":"https://example.com/grace.png"}}}`)
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()

	verifier := newFacebookTestVerifier(t, server, FacebookConfig{
		AppID: appID, AppSecret: appSecret, GraphVersion: "v24.0",
	})
	identity, err := verifier.Verify(context.Background(), Credential{Token: userToken})
	if err != nil {
		t.Fatal(err)
	}
	if profileCalls.Load() != 1 || identity.LoginID != "user-1" || identity.Provider != ProviderFacebook ||
		identity.Username == nil || *identity.Username != "Grace" ||
		identity.Gender == nil || *identity.Gender != GenderFemale || identity.EmailVerified != nil {
		t.Fatalf("unexpected identity: %#v", identity)
	}
}

func TestFacebookRejectsWrongAppBeforeProfile(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		fmt.Fprint(response, `{"data":{"app_id":"another-app","is_valid":true,"user_id":"user-1"}}`)
	}))
	defer server.Close()
	verifier := newFacebookTestVerifier(t, server, FacebookConfig{AppID: "app", AppSecret: "secret", GraphVersion: "v24.0"})
	_, err := verifier.Verify(context.Background(), Credential{Token: "token"})
	if !errors.Is(err, ErrInvalidCredential) || calls.Load() != 1 {
		t.Fatalf("Verify() = %v, calls = %d", err, calls.Load())
	}
}

func TestFacebookRejectsExpiredTokenAndMismatchedProfile(t *testing.T) {
	tests := []struct {
		name    string
		debug   string
		profile string
	}{
		{name: "expired", debug: `{"data":{"app_id":"app","is_valid":true,"user_id":"user-1","expires_at":99}}`},
		{name: "mismatch", debug: `{"data":{"app_id":"app","is_valid":true,"user_id":"user-1"}}`, profile: `{"id":"user-2"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				if strings.HasSuffix(request.URL.Path, "/debug_token") {
					fmt.Fprint(response, test.debug)
					return
				}
				fmt.Fprint(response, test.profile)
			}))
			defer server.Close()
			verifier := newFacebookTestVerifier(t, server, FacebookConfig{AppID: "app", AppSecret: "secret", GraphVersion: "v24.0"})
			verifier.now = func() time.Time { return time.Unix(100, 0) }
			if _, err := verifier.Verify(context.Background(), Credential{Token: "token"}); !errors.Is(err, ErrInvalidCredential) {
				t.Fatalf("Verify() error = %v", err)
			}
		})
	}
}

func TestFacebookClassifiesUpstreamAndHidesSecrets(t *testing.T) {
	const token = "secret-user-token"
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		http.Error(response, token, http.StatusServiceUnavailable)
	}))
	defer server.Close()
	verifier := newFacebookTestVerifier(t, server, FacebookConfig{AppID: "app", AppSecret: "secret", GraphVersion: "v24.0"})
	_, err := verifier.Verify(context.Background(), Credential{Token: token})
	if !errors.Is(err, ErrProviderUnavailable) || strings.Contains(err.Error(), token) {
		t.Fatalf("Verify() error = %v", err)
	}
}

func TestNewFacebookValidatesConfig(t *testing.T) {
	for _, config := range []FacebookConfig{
		{},
		{AppID: "app", AppSecret: "secret"},
		{AppID: "app", AppSecret: "secret", GraphVersion: "latest"},
	} {
		if _, err := NewFacebook(config); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("NewFacebook(%+v) error = %v", config, err)
		}
	}
}

func newFacebookTestVerifier(t *testing.T, server *httptest.Server, config FacebookConfig) *facebookVerifier {
	t.Helper()
	config.HTTPClient = server.Client()
	created, err := NewFacebook(config)
	if err != nil {
		t.Fatal(err)
	}
	verifier := created.(*facebookVerifier)
	verifier.baseURL = server.URL
	return verifier
}
