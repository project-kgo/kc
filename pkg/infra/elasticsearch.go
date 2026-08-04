package infra

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/project-kgo/kc/pkg/resource"
)

func validateElasticsearchConfig(config DataConfig) error {
	elasticsearchConfig := config.Elasticsearch
	if config.DSN != "" {
		if !validHTTPURL(config.DSN) {
			return fmt.Errorf("%w: invalid elasticsearch dsn", ErrInvalidConfig)
		}
	} else {
		if elasticsearchConfig == nil {
			return fmt.Errorf("%w: elasticsearch config is missing", ErrInvalidConfig)
		}
		if elasticsearchConfig.CloudID != "" && len(elasticsearchConfig.Addresses) > 0 {
			return fmt.Errorf("%w: elasticsearch cloud id conflicts with addresses", ErrInvalidConfig)
		}
		if elasticsearchConfig.CloudID == "" && len(elasticsearchConfig.Addresses) == 0 {
			return fmt.Errorf("%w: elasticsearch address is empty", ErrInvalidConfig)
		}
		for _, address := range elasticsearchConfig.Addresses {
			if !validHTTPURL(address) {
				return fmt.Errorf("%w: invalid elasticsearch address", ErrInvalidConfig)
			}
		}
	}
	if elasticsearchConfig != nil && elasticsearchConfig.APIKey != "" && (elasticsearchConfig.Username != "" || elasticsearchConfig.Password != "") {
		return fmt.Errorf("%w: elasticsearch api key conflicts with basic auth", ErrInvalidConfig)
	}
	if elasticsearchConfig != nil && elasticsearchConfig.Username == "" && elasticsearchConfig.Password != "" {
		return fmt.Errorf("%w: elasticsearch username is empty", ErrInvalidConfig)
	}
	return nil
}

func validHTTPURL(value string) bool {
	parsed, err := url.Parse(value)
	return err == nil && parsed.Host != "" && (parsed.Scheme == "http" || parsed.Scheme == "https")
}

func prepareElasticsearch(ctx context.Context, name string, config DataConfig) (preparedClient, error) {
	options := make([]elasticsearch.Option, 0, 8)
	elasticsearchConfig := config.Elasticsearch
	if config.DSN != "" {
		options = append(options, elasticsearch.WithAddresses(config.DSN))
	} else if elasticsearchConfig.CloudID != "" {
		options = append(options, elasticsearch.WithCloudID(elasticsearchConfig.CloudID))
	} else {
		addresses := append([]string(nil), elasticsearchConfig.Addresses...)
		options = append(options, elasticsearch.WithAddresses(addresses...))
	}

	if elasticsearchConfig != nil {
		if !dsnHasUser(config.DSN) {
			switch {
			case elasticsearchConfig.APIKey != "":
				options = append(options, elasticsearch.WithAPIKey(elasticsearchConfig.APIKey))
			case elasticsearchConfig.Username != "" || elasticsearchConfig.Password != "":
				options = append(options, elasticsearch.WithBasicAuth(elasticsearchConfig.Username, elasticsearchConfig.Password))
			}
		}
		if elasticsearchConfig.CertificateFingerprint != "" {
			options = append(options, elasticsearch.WithCertificateFingerprint(elasticsearchConfig.CertificateFingerprint))
		}
		if elasticsearchConfig.EnableCompression {
			options = append(options, elasticsearch.WithCompression())
		}
		if elasticsearchConfig.DiscoverNodesOnStart {
			options = append(options, elasticsearch.WithDiscoverNodesOnStart())
		}
	}

	client, err := elasticsearch.NewTyped(options...)
	if err != nil {
		return preparedClient{}, fmt.Errorf("create elasticsearch client: %w", err)
	}
	if !config.SkipCheck {
		checkCtx, cancel := checkContext(ctx, config.CheckTimeout)
		_, err = client.Info().Do(checkCtx)
		cancel()
		if err != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), defaultCheckTimeout)
			closeErr := client.Close(closeCtx)
			closeCancel()
			return preparedClient{}, errors.Join(fmt.Errorf("elasticsearch connection check: %w", err), closeErr)
		}
	}

	return preparedClient{
		name:   name,
		module: "data",
		typ:    string(config.Type),
		commit: func() error {
			return resource.Set(name, client)
		},
		close: func(ctx context.Context) error {
			return client.Close(ctx)
		},
	}, nil
}

func dsnHasUser(dsn string) bool {
	if strings.TrimSpace(dsn) == "" {
		return false
	}
	parsed, err := url.Parse(dsn)
	return err == nil && parsed.User != nil
}
