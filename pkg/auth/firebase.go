package auth

import (
	"context"
	"fmt"
	"strings"

	firebase "firebase.google.com/go/v4"
	firebaseauth "firebase.google.com/go/v4/auth"
	"google.golang.org/api/option"
)

const firebaseGoogleProvider = "google.com"

// FirebaseGoogleConfig 配置由 Firebase 签发的 Google 登录 ID Token 校验器。
type FirebaseGoogleConfig struct {
	ProjectID           string
	CredentialsJSON     []byte
	SkipRevocationCheck bool
}

type firebaseTokenClient interface {
	VerifyIDToken(context.Context, string) (*firebaseauth.Token, error)
	VerifyIDTokenAndCheckRevoked(context.Context, string) (*firebaseauth.Token, error)
}

type firebaseGoogleVerifier struct {
	client              firebaseTokenClient
	skipRevocationCheck bool
}

// NewFirebaseGoogle 创建仅接受 Firebase Google 登录来源的 Verifier。
// CredentialsJSON 为空时使用 Google Application Default Credentials。
func NewFirebaseGoogle(ctx context.Context, config FirebaseGoogleConfig) (Verifier, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: firebase context is nil", ErrInvalidConfig)
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("%w: firebase context is done", ErrInvalidConfig)
	}
	if strings.TrimSpace(config.ProjectID) == "" {
		return nil, fmt.Errorf("%w: firebase project ID is empty", ErrInvalidConfig)
	}

	var options []option.ClientOption
	if len(config.CredentialsJSON) > 0 {
		options = append(options, option.WithCredentialsJSON(config.CredentialsJSON))
	}
	app, err := firebase.NewApp(ctx, &firebase.Config{ProjectID: config.ProjectID}, options...)
	if err != nil {
		return nil, fmt.Errorf("%w: cannot initialize firebase app", ErrInvalidConfig)
	}
	client, err := app.Auth(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: cannot initialize firebase auth client", ErrInvalidConfig)
	}
	return newFirebaseGoogleVerifier(client, config.SkipRevocationCheck), nil
}

func newFirebaseGoogleVerifier(client firebaseTokenClient, skipRevocationCheck bool) Verifier {
	return &firebaseGoogleVerifier{client: client, skipRevocationCheck: skipRevocationCheck}
}

func (v *firebaseGoogleVerifier) Provider() Provider {
	return ProviderGoogle
}

func (v *firebaseGoogleVerifier) Verify(ctx context.Context, credential Credential) (*Identity, error) {
	if err := validateVerifierInput(ctx, credential); err != nil {
		return nil, err
	}
	if credential.ExpectedNonce != nil {
		return nil, fmt.Errorf("%w: google does not support expected nonce", ErrInvalidArgument)
	}

	var (
		token *firebaseauth.Token
		err   error
	)
	if v.skipRevocationCheck {
		token, err = v.client.VerifyIDToken(ctx, credential.Token)
	} else {
		token, err = v.client.VerifyIDTokenAndCheckRevoked(ctx, credential.Token)
	}
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if firebaseauth.IsIDTokenInvalid(err) || firebaseauth.IsUserNotFound(err) {
			return nil, fmt.Errorf("%w: google token verification failed", ErrInvalidCredential)
		}
		return nil, fmt.Errorf("%w: firebase token verification failed", ErrProviderUnavailable)
	}
	if token == nil || strings.TrimSpace(token.UID) == "" || token.Firebase.SignInProvider != firebaseGoogleProvider {
		return nil, fmt.Errorf("%w: token is not a Firebase Google identity", ErrInvalidCredential)
	}

	identity := &Identity{
		Provider:  ProviderGoogle,
		LoginID:   token.UID,
		Username:  claimString(token.Claims, "name"),
		AvatarURL: claimString(token.Claims, "picture"),
		Email:     claimString(token.Claims, "email"),
	}
	if verified, ok := token.Claims["email_verified"].(bool); ok {
		identity.EmailVerified = boolPointer(verified)
	}
	return identity, nil
}

func claimString(claims map[string]interface{}, name string) *string {
	value, ok := claims[name].(string)
	if !ok {
		return nil
	}
	return stringPointer(value)
}
