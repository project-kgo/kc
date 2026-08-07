package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const (
	facebookGraphBaseURL = "https://graph.facebook.com"
	maxProviderResponse  = 1 << 20
	defaultHTTPTimeout   = 10 * time.Second
)

var facebookVersionPattern = regexp.MustCompile(`^v[0-9]+\.[0-9]+$`)

// FacebookConfig 配置 Facebook Graph API 登录校验器。
type FacebookConfig struct {
	AppID        string
	AppSecret    string
	GraphVersion string
	HTTPClient   *http.Client
}

type facebookVerifier struct {
	appID        string
	appSecret    string
	graphVersion string
	httpClient   *http.Client
	baseURL      string
	now          func() time.Time
}

// NewFacebook 创建 Facebook Access Token Verifier。
func NewFacebook(config FacebookConfig) (Verifier, error) {
	if strings.TrimSpace(config.AppID) == "" || config.AppSecret == "" {
		return nil, fmt.Errorf("%w: facebook app ID or app secret is empty", ErrInvalidConfig)
	}
	if !facebookVersionPattern.MatchString(config.GraphVersion) {
		return nil, fmt.Errorf("%w: facebook graph version is invalid", ErrInvalidConfig)
	}
	client := config.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: defaultHTTPTimeout}
	}
	return &facebookVerifier{
		appID:        config.AppID,
		appSecret:    config.AppSecret,
		graphVersion: config.GraphVersion,
		httpClient:   client,
		baseURL:      facebookGraphBaseURL,
		now:          time.Now,
	}, nil
}

func (v *facebookVerifier) Provider() Provider {
	return ProviderFacebook
}

func (v *facebookVerifier) Verify(ctx context.Context, credential Credential) (*Identity, error) {
	if err := validateVerifierInput(ctx, credential); err != nil {
		return nil, err
	}
	if credential.ExpectedNonce != nil {
		return nil, fmt.Errorf("%w: facebook does not support expected nonce", ErrInvalidArgument)
	}
	debug, err := v.debugToken(ctx, credential.Token)
	if err != nil {
		return nil, err
	}
	if !debug.IsValid || debug.AppID != v.appID || strings.TrimSpace(debug.UserID) == "" {
		return nil, fmt.Errorf("%w: facebook token is invalid or belongs to another app", ErrInvalidCredential)
	}
	now := v.now().Unix()
	if (debug.ExpiresAt > 0 && debug.ExpiresAt <= now) ||
		(debug.DataAccessExpiresAt > 0 && debug.DataAccessExpiresAt <= now) {
		return nil, fmt.Errorf("%w: facebook token is expired", ErrInvalidCredential)
	}

	profile, err := v.profile(ctx, credential.Token)
	if err != nil {
		return nil, err
	}
	if profile.ID != debug.UserID {
		return nil, fmt.Errorf("%w: facebook profile does not match token", ErrInvalidCredential)
	}

	return &Identity{
		Provider:  ProviderFacebook,
		LoginID:   profile.ID,
		Username:  stringPointer(profile.Name),
		AvatarURL: stringPointer(profile.Picture.Data.URL),
		Gender:    normalizeGender(profile.Gender),
		Email:     stringPointer(profile.Email),
	}, nil
}

type facebookDebugResponse struct {
	Data struct {
		AppID               string `json:"app_id"`
		DataAccessExpiresAt int64  `json:"data_access_expiration_time"`
		ExpiresAt           int64  `json:"expires_at"`
		IsValid             bool   `json:"is_valid"`
		UserID              string `json:"user_id"`
	} `json:"data"`
}

type facebookDebugData struct {
	AppID               string
	DataAccessExpiresAt int64
	ExpiresAt           int64
	IsValid             bool
	UserID              string
}

func (v *facebookVerifier) debugToken(ctx context.Context, token string) (facebookDebugData, error) {
	values := url.Values{"input_token": {token}}
	endpoint := v.endpoint("debug_token") + "?" + values.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return facebookDebugData{}, fmt.Errorf("%w: cannot create facebook request", ErrInvalidConfig)
	}
	request.Header.Set("Authorization", "Bearer "+v.appID+"|"+v.appSecret)

	var response facebookDebugResponse
	if err := v.doJSON(request, &response); err != nil {
		return facebookDebugData{}, err
	}
	return facebookDebugData{
		AppID:               response.Data.AppID,
		DataAccessExpiresAt: response.Data.DataAccessExpiresAt,
		ExpiresAt:           response.Data.ExpiresAt,
		IsValid:             response.Data.IsValid,
		UserID:              response.Data.UserID,
	}, nil
}

type facebookProfile struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Email   string `json:"email"`
	Gender  string `json:"gender"`
	Picture struct {
		Data struct {
			URL string `json:"url"`
		} `json:"data"`
	} `json:"picture"`
}

func (v *facebookVerifier) profile(ctx context.Context, token string) (facebookProfile, error) {
	values := url.Values{
		"fields":          {"id,name,picture,email,gender"},
		"appsecret_proof": {facebookAppSecretProof(token, v.appSecret)},
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, v.endpoint("me")+"?"+values.Encode(), nil)
	if err != nil {
		return facebookProfile{}, fmt.Errorf("%w: cannot create facebook request", ErrInvalidConfig)
	}
	request.Header.Set("Authorization", "Bearer "+token)

	var profile facebookProfile
	if err := v.doJSON(request, &profile); err != nil {
		return facebookProfile{}, err
	}
	if strings.TrimSpace(profile.ID) == "" {
		return facebookProfile{}, fmt.Errorf("%w: facebook profile has no ID", ErrProviderUnavailable)
	}
	return profile, nil
}

func (v *facebookVerifier) endpoint(operation string) string {
	return strings.TrimRight(v.baseURL, "/") + "/" + v.graphVersion + "/" + operation
}

func (v *facebookVerifier) doJSON(request *http.Request, target any) error {
	response, err := v.httpClient.Do(request)
	if err != nil {
		if ctxErr := request.Context().Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("%w: facebook request failed", ErrProviderUnavailable)
	}
	defer response.Body.Close()

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxProviderResponse))
		if response.StatusCode == http.StatusBadRequest || response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
			return fmt.Errorf("%w: facebook rejected the token", ErrInvalidCredential)
		}
		return fmt.Errorf("%w: facebook returned status %d", ErrProviderUnavailable, response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxProviderResponse+1))
	if err != nil || len(body) > maxProviderResponse {
		return fmt.Errorf("%w: facebook response is too large", ErrProviderUnavailable)
	}
	if err := json.Unmarshal(body, target); err != nil {
		return fmt.Errorf("%w: facebook returned invalid JSON", ErrProviderUnavailable)
	}
	return nil
}

func facebookAppSecretProof(token, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(token))
	return hex.EncodeToString(mac.Sum(nil))
}
