// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kafka // import "github.com/open-telemetry/opentelemetry-collector-contrib/internal/kafka"

import (
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"
	"go.opentelemetry.io/collector/config/configtls"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/kafka/awsmsk"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/kafka/configkafka"
)

// ConfigureAuthentication configures authentication in sarama.Config.
func ConfigureSaramaAuthentication(
	ctx context.Context,
	config configkafka.AuthenticationConfig,
	saramaConfig *sarama.Config,
) error {
	if config.PlainText != nil {
		configurePlaintext(*config.PlainText, saramaConfig)
	}
	if config.TLS != nil {
		if err := configureTLS(*config.TLS, saramaConfig); err != nil {
			return err
		}
	}
	if config.SASL != nil {
		if err := configureSASL(ctx, *config.SASL, saramaConfig); err != nil {
			return err
		}
	}

	if config.Kerberos != nil {
		configureKerberos(*config.Kerberos, saramaConfig)
	}
	return nil
}

func configurePlaintext(config configkafka.PlainTextConfig, saramaConfig *sarama.Config) {
	saramaConfig.Net.SASL.Enable = true
	saramaConfig.Net.SASL.User = config.Username
	saramaConfig.Net.SASL.Password = config.Password
}

func configureSASL(ctx context.Context, config configkafka.SASLConfig, saramaConfig *sarama.Config) error {
	if config.Username == "" && config.Mechanism != "AWS_MSK_IAM_OAUTHBEARER" {
		return fmt.Errorf("username have to be provided")
	}

	if config.Password == "" && config.Mechanism != "AWS_MSK_IAM_OAUTHBEARER" {
		return fmt.Errorf("password have to be provided")
	}

	saramaConfig.Net.SASL.Enable = true
	saramaConfig.Net.SASL.User = config.Username
	saramaConfig.Net.SASL.Password = config.Password

	switch config.Mechanism {
	case "SCRAM-SHA-512":
		saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: sha512.New} }
		saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	case "SCRAM-SHA-256":
		saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: sha256.New} }
		saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
	case "PLAIN":
		saramaConfig.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	case "AWS_MSK_IAM":
		saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return awsmsk.NewIAMSASLClient(config.AWSMSK.BrokerAddr, config.AWSMSK.Region, saramaConfig.ClientID)
		}
		saramaConfig.Net.SASL.Mechanism = awsmsk.Mechanism
	case "AWS_MSK_IAM_OAUTHBEARER":
		saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeOAuth
		saramaConfig.Net.SASL.TokenProvider = &awsMSKTokenProvider{ctx: ctx, region: config.AWSMSK.Region}
		tlsConfig := tls.Config{}
		saramaConfig.Net.TLS.Enable = true
		saramaConfig.Net.TLS.Config = &tlsConfig
	default:
		return fmt.Errorf(`invalid SASL Mechanism %q: can be either "PLAIN", "AWS_MSK_IAM", "AWS_MSK_IAM_OAUTHBEARER", "SCRAM-SHA-256" or "SCRAM-SHA-512"`, config.Mechanism)
	}

	switch config.Version {
	case 0:
		saramaConfig.Net.SASL.Version = sarama.SASLHandshakeV0
	case 1:
		saramaConfig.Net.SASL.Version = sarama.SASLHandshakeV1
	default:
		return fmt.Errorf(`invalid SASL Protocol Version %d: can be either 0 or 1`, config.Version)
	}

	return nil
}

func configureTLS(config configtls.ClientConfig, saramaConfig *sarama.Config) error {
	tlsConfig, err := config.LoadTLSConfig(context.Background())
	if err != nil {
		return fmt.Errorf("error loading tls config: %w", err)
	}
	saramaConfig.Net.TLS.Enable = true
	saramaConfig.Net.TLS.Config = tlsConfig
	return nil
}

func configureKerberos(config configkafka.KerberosConfig, saramaConfig *sarama.Config) {
	saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeGSSAPI
	saramaConfig.Net.SASL.Enable = true
	if config.UseKeyTab {
		saramaConfig.Net.SASL.GSSAPI.KeyTabPath = config.KeyTabPath
		saramaConfig.Net.SASL.GSSAPI.AuthType = sarama.KRB5_KEYTAB_AUTH
	} else {
		saramaConfig.Net.SASL.GSSAPI.AuthType = sarama.KRB5_USER_AUTH
		saramaConfig.Net.SASL.GSSAPI.Password = config.Password
	}
	saramaConfig.Net.SASL.GSSAPI.KerberosConfigPath = config.ConfigPath
	saramaConfig.Net.SASL.GSSAPI.Username = config.Username
	saramaConfig.Net.SASL.GSSAPI.Realm = config.Realm
	saramaConfig.Net.SASL.GSSAPI.ServiceName = config.ServiceName
	saramaConfig.Net.SASL.GSSAPI.DisablePAFXFAST = config.DisablePAFXFAST
}

type awsMSKTokenProvider struct {
	ctx    context.Context
	region string
}

// Token return the AWS session token for the AWS_MSK_IAM_OAUTHBEARER mechanism
func (c *awsMSKTokenProvider) Token() (*sarama.AccessToken, error) {
	token, _, err := signer.GenerateAuthToken(c.ctx, c.region)
	return &sarama.AccessToken{Token: token}, err
}
