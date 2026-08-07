# Auth

`auth` 用于在 Go 服务端校验客户端已经取得的第三方登录凭证，并返回统一身份模型。它不处理浏览器跳转、OAuth `state`、账号创建、账号绑定或数据库存储。

## 安装与配置

```go
package main

import (
	"context"
	"log"

	"github.com/project-kgo/kc/pkg/auth"
)

func buildAuth(ctx context.Context) *auth.Authenticator {
	google, err := auth.NewFirebaseGoogle(ctx, auth.FirebaseGoogleConfig{
		ProjectID: "my-firebase-project",
		// 默认使用 ADC；也可通过 CredentialsJSON 传入服务账号 JSON。
		// 默认会检查 token 撤销和用户禁用状态。
	})
	if err != nil {
		log.Fatal(err)
	}

	facebook, err := auth.NewFacebook(auth.FacebookConfig{
		AppID:        "facebook-app-id",
		AppSecret:    "facebook-app-secret",
		GraphVersion: "v24.0", // 由应用明确选择当前启用的 Graph API 版本。
	})
	if err != nil {
		log.Fatal(err)
	}

	apple, err := auth.NewApple(auth.AppleConfig{
		ClientIDs: []string{"com.example.app", "com.example.web"},
	})
	if err != nil {
		log.Fatal(err)
	}

	authenticator, err := auth.New(google, facebook, apple)
	if err != nil {
		log.Fatal(err)
	}
	return authenticator
}
```

## 校验凭证

```go
identity, err := authenticator.Authenticate(ctx, auth.Credential{
	Provider: auth.ProviderApple,
	Token:    identityToken,
	// 该值必须来自服务端保存的登录挑战，不能直接信任客户端回传值。
	ExpectedNonce: &expectedNonce,
})
if err != nil {
	// 可用 errors.Is 区分 ErrInvalidCredential、ErrProviderUnavailable 等错误。
	return err
}

// Provider 与 LoginID 共同构成稳定的外部身份键。
externalKey := string(identity.Provider) + ":" + identity.LoginID
```

Google 和 Apple 的 `Token` 是 ID Token；Facebook 的 `Token` 是用户 Access Token。Google verifier 只接受 Firebase claim `sign_in_provider=google.com`，不会接受密码、匿名或其他 Firebase 登录方式。

## 字段可用性

| 字段 | Firebase Google | Facebook | Apple |
| --- | --- | --- | --- |
| `Provider` / `LoginID` | 必有 | 必有 | 必有 |
| `Username` | `name` claim | `name` | 通常没有 |
| `AvatarURL` | `picture` claim | `picture` | 没有 |
| `Gender` | 没有 | 有权限时提供 | 没有 |
| `Email` | 有权限时提供 | 有权限时提供 | token 包含时提供 |
| `EmailVerified` | token 包含时提供 | 不提供 | token 包含时提供 |

所有可选字段均为指针；`nil` 表示平台没有提供，不能把它解释为空字符串或 `false`。Apple 的姓名只在首次授权响应中由客户端提供，并不在后续 Identity Token 中稳定出现，因此本组件不会把未签名的客户端姓名写入统一身份。

## 安全注意事项

- 不要记录 `Credential`、Facebook App Secret 或 Firebase 服务账号 JSON。
- Apple `ExpectedNonce` 应来自服务端创建并保存的挑战，其值应与 token 中的 `nonce` claim 完全一致。
- Firebase 默认执行撤销检查，每次登录会额外访问 Firebase Auth；仅在明确接受该风险时设置 `SkipRevocationCheck`。
- Facebook 会先校验 token 的 App 归属，再读取用户资料；Graph API 版本必须由应用显式配置并按 Meta 生命周期升级。
- 本组件只证明第三方身份，不决定多个 Provider 是否属于同一个业务用户；账号绑定必须由业务层在重新认证后完成。
