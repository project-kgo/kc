// Package auth 提供第三方登录凭证校验和统一身份模型。
package auth

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

const maxCredentialLength = 64 << 10

var (
	// ErrInvalidConfig 表示 verifier 配置不完整或互相冲突。
	ErrInvalidConfig = errors.New("auth: invalid config")
	// ErrInvalidArgument 表示调用参数不合法。
	ErrInvalidArgument = errors.New("auth: invalid argument")
	// ErrUnsupportedProvider 表示请求的登录平台没有注册 verifier。
	ErrUnsupportedProvider = errors.New("auth: unsupported provider")
	// ErrInvalidCredential 表示第三方凭证无效、过期或不属于当前应用。
	ErrInvalidCredential = errors.New("auth: invalid credential")
	// ErrProviderUnavailable 表示第三方服务暂时不可用或返回了无法处理的响应。
	ErrProviderUnavailable = errors.New("auth: provider unavailable")
)

// Provider 标识第三方登录平台。LoginID 仅在同一个 Provider 内唯一。
type Provider string

const (
	ProviderGoogle   Provider = "google"
	ProviderFacebook Provider = "facebook"
	ProviderApple    Provider = "apple"
)

// Gender 是跨平台归一化后的性别。
type Gender string

const (
	GenderMale   Gender = "male"
	GenderFemale Gender = "female"
	GenderOther  Gender = "other"
)

// Identity 是验证成功后的统一身份模型。
// Username 表示平台展示名称，不保证是唯一用户名。
type Identity struct {
	Provider      Provider `json:"provider"`
	LoginID       string   `json:"login_id"`
	Username      *string  `json:"username,omitempty"`
	AvatarURL     *string  `json:"avatar_url,omitempty"`
	Gender        *Gender  `json:"gender,omitempty"`
	Email         *string  `json:"email,omitempty"`
	EmailVerified *bool    `json:"email_verified,omitempty"`
}

// Credential 包含客户端从第三方平台取得的凭证。
// Google 和 Apple 使用 ID Token，Facebook 使用 Access Token。
type Credential struct {
	Provider      Provider `json:"provider"`
	Token         string   `json:"token"`
	ExpectedNonce *string  `json:"expected_nonce,omitempty"`
}

// Verifier 校验某个平台的凭证。实现必须支持并发调用。
type Verifier interface {
	Provider() Provider
	Verify(context.Context, Credential) (*Identity, error)
}

// Authenticator 按 Provider 将凭证路由给已注册的 Verifier。
type Authenticator struct {
	verifiers map[Provider]Verifier
}

// New 创建统一认证器。每个 Provider 只能注册一个 Verifier。
func New(verifiers ...Verifier) (*Authenticator, error) {
	if len(verifiers) == 0 {
		return nil, fmt.Errorf("%w: at least one verifier is required", ErrInvalidConfig)
	}

	registered := make(map[Provider]Verifier, len(verifiers))
	for index, verifier := range verifiers {
		if isNil(verifier) {
			return nil, fmt.Errorf("%w: verifier %d is nil", ErrInvalidConfig, index)
		}
		provider := verifier.Provider()
		if strings.TrimSpace(string(provider)) == "" {
			return nil, fmt.Errorf("%w: verifier %d has an empty provider", ErrInvalidConfig, index)
		}
		if _, exists := registered[provider]; exists {
			return nil, fmt.Errorf("%w: duplicate provider %q", ErrInvalidConfig, provider)
		}
		registered[provider] = verifier
	}

	return &Authenticator{verifiers: registered}, nil
}

// Authenticate 校验凭证并返回统一身份信息。
func (a *Authenticator) Authenticate(ctx context.Context, credential Credential) (*Identity, error) {
	if a == nil {
		return nil, fmt.Errorf("%w: authenticator is nil", ErrInvalidArgument)
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(string(credential.Provider)) == "" {
		return nil, fmt.Errorf("%w: provider is empty", ErrInvalidArgument)
	}
	if credential.Token == "" || len(credential.Token) > maxCredentialLength {
		return nil, fmt.Errorf("%w: token is empty or too large", ErrInvalidArgument)
	}
	if credential.ExpectedNonce != nil && *credential.ExpectedNonce == "" {
		return nil, fmt.Errorf("%w: expected nonce is empty", ErrInvalidArgument)
	}

	verifier, ok := a.verifiers[credential.Provider]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedProvider, credential.Provider)
	}
	identity, err := verifier.Verify(ctx, credential)
	if err != nil {
		return nil, err
	}
	if identity == nil || identity.Provider != credential.Provider || strings.TrimSpace(identity.LoginID) == "" {
		return nil, fmt.Errorf("%w: %s returned an invalid identity", ErrProviderUnavailable, credential.Provider)
	}
	return identity, nil
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func stringPointer(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func boolPointer(value bool) *bool {
	return &value
}

func normalizeGender(value string) *Gender {
	var gender Gender
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "":
		return nil
	case "male":
		gender = GenderMale
	case "female":
		gender = GenderFemale
	default:
		gender = GenderOther
	}
	return &gender
}

func validateVerifierInput(ctx context.Context, credential Credential) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if credential.Token == "" || len(credential.Token) > maxCredentialLength {
		return fmt.Errorf("%w: token is empty or too large", ErrInvalidArgument)
	}
	if credential.ExpectedNonce != nil && *credential.ExpectedNonce == "" {
		return fmt.Errorf("%w: expected nonce is empty", ErrInvalidArgument)
	}
	return nil
}
