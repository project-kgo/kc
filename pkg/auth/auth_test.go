package auth

import (
	"context"
	"errors"
	"testing"
)

type stubVerifier struct {
	provider Provider
	identity *Identity
	err      error
}

func (v *stubVerifier) Provider() Provider {
	return v.provider
}

func (v *stubVerifier) Verify(context.Context, Credential) (*Identity, error) {
	return v.identity, v.err
}

func TestNewAndAuthenticate(t *testing.T) {
	want := &Identity{Provider: ProviderGoogle, LoginID: "uid-1"}
	authenticator, err := New(&stubVerifier{provider: ProviderGoogle, identity: want})
	if err != nil {
		t.Fatal(err)
	}
	got, err := authenticator.Authenticate(context.Background(), Credential{
		Provider: ProviderGoogle,
		Token:    "valid-token",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("Authenticate() = %#v, want original identity", got)
	}
}

func TestNewRejectsInvalidVerifiers(t *testing.T) {
	var typedNil *stubVerifier
	tests := []struct {
		name      string
		verifiers []Verifier
	}{
		{name: "empty"},
		{name: "nil", verifiers: []Verifier{nil}},
		{name: "typed nil", verifiers: []Verifier{typedNil}},
		{name: "empty provider", verifiers: []Verifier{&stubVerifier{}}},
		{name: "duplicate", verifiers: []Verifier{
			&stubVerifier{provider: ProviderGoogle},
			&stubVerifier{provider: ProviderGoogle},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := New(test.verifiers...); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("New() error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestAuthenticateRejectsInvalidArgumentsAndProvider(t *testing.T) {
	authenticator, err := New(&stubVerifier{provider: ProviderGoogle, identity: &Identity{
		Provider: ProviderGoogle,
		LoginID:  "uid",
	}})
	if err != nil {
		t.Fatal(err)
	}
	empty := ""
	tests := []struct {
		name       string
		auth       *Authenticator
		ctx        context.Context
		credential Credential
		want       error
	}{
		{name: "nil authenticator", ctx: context.Background(), credential: Credential{Provider: ProviderGoogle, Token: "x"}, want: ErrInvalidArgument},
		{name: "nil context", auth: authenticator, credential: Credential{Provider: ProviderGoogle, Token: "x"}, want: ErrInvalidArgument},
		{name: "empty provider", auth: authenticator, ctx: context.Background(), credential: Credential{Token: "x"}, want: ErrInvalidArgument},
		{name: "empty token", auth: authenticator, ctx: context.Background(), credential: Credential{Provider: ProviderGoogle}, want: ErrInvalidArgument},
		{name: "empty nonce", auth: authenticator, ctx: context.Background(), credential: Credential{Provider: ProviderGoogle, Token: "x", ExpectedNonce: &empty}, want: ErrInvalidArgument},
		{name: "unsupported", auth: authenticator, ctx: context.Background(), credential: Credential{Provider: ProviderApple, Token: "x"}, want: ErrUnsupportedProvider},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := test.auth.Authenticate(test.ctx, test.credential)
			if !errors.Is(err, test.want) {
				t.Fatalf("Authenticate() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestAuthenticatePreservesCancellationAndValidatesIdentity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	authenticator, _ := New(&stubVerifier{provider: ProviderGoogle})
	if _, err := authenticator.Authenticate(ctx, Credential{Provider: ProviderGoogle, Token: "x"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled error = %v", err)
	}

	tests := []struct {
		name     string
		identity *Identity
	}{
		{name: "nil"},
		{name: "wrong provider", identity: &Identity{Provider: ProviderApple, LoginID: "uid"}},
		{name: "empty login ID", identity: &Identity{Provider: ProviderGoogle}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authenticator, _ := New(&stubVerifier{provider: ProviderGoogle, identity: test.identity})
			_, err := authenticator.Authenticate(context.Background(), Credential{Provider: ProviderGoogle, Token: "x"})
			if !errors.Is(err, ErrProviderUnavailable) {
				t.Fatalf("Authenticate() error = %v, want ErrProviderUnavailable", err)
			}
		})
	}
}

func TestNormalizeGender(t *testing.T) {
	for input, want := range map[string]*Gender{
		"":       nil,
		"male":   genderPointer(GenderMale),
		"FEMALE": genderPointer(GenderFemale),
		"custom": genderPointer(GenderOther),
	} {
		got := normalizeGender(input)
		if (got == nil) != (want == nil) || (got != nil && *got != *want) {
			t.Fatalf("normalizeGender(%q) = %v, want %v", input, got, want)
		}
	}
}

func genderPointer(value Gender) *Gender {
	return &value
}
