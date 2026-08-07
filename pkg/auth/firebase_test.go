package auth

import (
	"context"
	"errors"
	"strings"
	"testing"

	firebaseauth "firebase.google.com/go/v4/auth"
)

type fakeFirebaseClient struct {
	token        *firebaseauth.Token
	err          error
	verifyCalls  int
	revokedCalls int
}

func (c *fakeFirebaseClient) VerifyIDToken(context.Context, string) (*firebaseauth.Token, error) {
	c.verifyCalls++
	return c.token, c.err
}

func (c *fakeFirebaseClient) VerifyIDTokenAndCheckRevoked(context.Context, string) (*firebaseauth.Token, error) {
	c.revokedCalls++
	return c.token, c.err
}

func TestFirebaseGoogleMapsIdentityAndChecksRevocation(t *testing.T) {
	client := &fakeFirebaseClient{token: &firebaseauth.Token{
		UID:      "firebase-uid",
		Firebase: firebaseauth.FirebaseInfo{SignInProvider: firebaseGoogleProvider},
		Claims: map[string]interface{}{
			"name":           "Ada",
			"picture":        "https://example.com/avatar.png",
			"email":          "ada@example.com",
			"email_verified": false,
		},
	}}
	verifier := newFirebaseGoogleVerifier(client, false)
	identity, err := verifier.Verify(context.Background(), Credential{Token: "secret-token"})
	if err != nil {
		t.Fatal(err)
	}
	if client.revokedCalls != 1 || client.verifyCalls != 0 {
		t.Fatalf("verification calls = (%d, %d), want revoked check", client.verifyCalls, client.revokedCalls)
	}
	if identity.Provider != ProviderGoogle || identity.LoginID != "firebase-uid" ||
		identity.Username == nil || *identity.Username != "Ada" ||
		identity.EmailVerified == nil || *identity.EmailVerified {
		t.Fatalf("unexpected identity: %#v", identity)
	}
}

func TestFirebaseGoogleCanSkipRevocationCheck(t *testing.T) {
	client := &fakeFirebaseClient{token: &firebaseauth.Token{
		UID:      "uid",
		Firebase: firebaseauth.FirebaseInfo{SignInProvider: firebaseGoogleProvider},
	}}
	verifier := newFirebaseGoogleVerifier(client, true)
	if _, err := verifier.Verify(context.Background(), Credential{Token: "token"}); err != nil {
		t.Fatal(err)
	}
	if client.verifyCalls != 1 || client.revokedCalls != 0 {
		t.Fatalf("verification calls = (%d, %d), want normal verification", client.verifyCalls, client.revokedCalls)
	}
}

func TestFirebaseGoogleRejectsOtherSignInProviders(t *testing.T) {
	client := &fakeFirebaseClient{token: &firebaseauth.Token{
		UID:      "uid",
		Firebase: firebaseauth.FirebaseInfo{SignInProvider: "password"},
	}}
	verifier := newFirebaseGoogleVerifier(client, true)
	if _, err := verifier.Verify(context.Background(), Credential{Token: "token"}); !errors.Is(err, ErrInvalidCredential) {
		t.Fatalf("Verify() error = %v, want ErrInvalidCredential", err)
	}
}

func TestFirebaseGoogleHidesErrorsAndRejectsNonce(t *testing.T) {
	const secret = "very-secret-firebase-token"
	client := &fakeFirebaseClient{err: errors.New(secret)}
	verifier := newFirebaseGoogleVerifier(client, true)
	_, err := verifier.Verify(context.Background(), Credential{Token: secret})
	if !errors.Is(err, ErrProviderUnavailable) || strings.Contains(err.Error(), secret) {
		t.Fatalf("Verify() error = %v", err)
	}
	nonce := "nonce"
	if _, err := verifier.Verify(context.Background(), Credential{Token: secret, ExpectedNonce: &nonce}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nonce error = %v", err)
	}
}

func TestNewFirebaseGoogleValidatesConfig(t *testing.T) {
	if _, err := NewFirebaseGoogle(nil, FirebaseGoogleConfig{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil context error = %v", err)
	}
	if _, err := NewFirebaseGoogle(context.Background(), FirebaseGoogleConfig{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("empty project error = %v", err)
	}
}
