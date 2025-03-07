// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package configkafka // import "github.com/open-telemetry/opentelemetry-collector-contrib/internal/kafka/configkafka"

import (
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/config/configtls"
)

const (
	LatestOffset   = "latest"
	EarliestOffset = "earliest"
)

type ClientConfig struct {
	// Brokers holds the list of Kafka bootstrap servers (default localhost:9092).
	Brokers []string `mapstructure:"brokers"`

	// ResolveCanonicalBootstrapServersOnly configures the Kafka client to perform
	// a DNS lookup on each of the provided brokers, and then perform a reverse
	// lookup on the resulting IPs to obtain the canonical hostnames to use as the
	// bootstrap servers. This can be required in SASL environments.
	ResolveCanonicalBootstrapServersOnly bool `mapstructure:"resolve_canonical_bootstrap_servers_only"`

	// ProtocolVersion defines the Kafka protocol version that the client will
	// assume it is running against.
	ProtocolVersion string `mapstructure:"protocol_version"`

	// ClientID holds the client ID advertised to Kafka, which can be used for
	// enforcing ACLs, throttling quotas, and more (default "otel-collector")
	ClientID string `mapstructure:"client_id"`

	// Authentication holds Kafka authentication details.
	Authentication AuthenticationConfig `mapstructure:"auth"`

	// Metadata holds metadata-related configuration for producers and consumers.
	Metadata MetadataConfig `mapstructure:"metadata"`
}

func NewDefaultClientConfig() ClientConfig {
	return ClientConfig{
		Brokers:  []string{"localhost:9092"},
		ClientID: "otel-collector",
		Metadata: MetadataConfig{
			Full: true,
			Retry: MetadataRetryConfig{
				Max:     3,
				Backoff: time.Millisecond * 250,
			},
		},
	}
}

func (c ClientConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("brokers must be specified")
	}
	return nil
}

type ConsumerConfig struct {
	// SessionTimeout controls the Kafka consumer group session timeout.
	// The session timeout is used to detect the consumer's liveness.
	SessionTimeout time.Duration `mapstructure:"session_timeout"`

	// HeartbeatInterval controls the Kafka consumer group coordination
	// heartbeat interval. Heartbeats ensure the consumer's session remains
	// active.
	HeartbeatInterval time.Duration `mapstructure:"heartbeat_interval"`

	// GroupID specifies the ID of the consumer group that will be
	// consuming messages from (default "otel-collector").
	GroupID string `mapstructure:"group_id"`

	// InitialOffset specifies the initial offset to use if no offset was
	// previously committed. Must be `latest` or `earliest` (default "latest").
	InitialOffset string `mapstructure:"initial_offset"`

	// AutoCommit controls the auto-commit functionality of the consumer.
	AutoCommit AutoCommitConfig `mapstructure:"autocommit"`

	// The minimum bytes per fetch from Kafka (default "1")
	MinFetchSize int32 `mapstructure:"min_fetch_size"`

	// The default bytes per fetch from Kafka (default "1048576")
	DefaultFetchSize int32 `mapstructure:"default_fetch_size"`

	// The maximum bytes per fetch from Kafka (default "0", no limit)
	MaxFetchSize int32 `mapstructure:"max_fetch_size"`
}

func NewDefaultConsumerConfig() ConsumerConfig {
	return ConsumerConfig{
		SessionTimeout:    10 * time.Second,
		HeartbeatInterval: 3 * time.Second,
		GroupID:           "otel-collector",
		InitialOffset:     "latest",
		AutoCommit: AutoCommitConfig{
			Enable:   true,
			Interval: time.Second,
		},
		MinFetchSize:     1,
		MaxFetchSize:     0,
		DefaultFetchSize: 1048576,
	}
}

func (c ConsumerConfig) Validate() error {
	switch c.InitialOffset {
	case LatestOffset, EarliestOffset:
		// Valid
	default:
		return fmt.Errorf(
			"initial_offset should be one of 'latest' or 'earliest'. configured value %v",
			c.InitialOffset,
		)
	}
	return nil
}

type AutoCommitConfig struct {
	// Whether or not to auto-commit updated offsets back to the broker.
	// (default enabled).
	Enable bool `mapstructure:"enable"`

	// How frequently to commit updated offsets. Ineffective unless
	// auto-commit is enabled (default 1s)
	Interval time.Duration `mapstructure:"interval"`
}

type MessageMarkingConfig struct {
	// If true, the messages are marked after the pipeline execution
	After bool `mapstructure:"after"`

	// If false, only the successfully processed messages are marked, it has no impact if
	// After is set to false.
	// Note: this can block the entire partition in case a message processing returns
	// a permanent error.
	OnError bool `mapstructure:"on_error"`
}

type HeaderExtractionConfig struct {
	ExtractHeaders bool     `mapstructure:"extract_headers"`
	Headers        []string `mapstructure:"headers"`
}

type ProducerConfig struct {
	// Maximum message bytes the producer will accept to produce (default 1000000)
	MaxMessageBytes int `mapstructure:"max_message_bytes"`

	// RequiredAcks Number of acknowledgements required to assume that a message has been sent.
	// https://pkg.go.dev/github.com/IBM/sarama@v1.30.0#RequiredAcks
	// The options are:
	//   0 -> NoResponse.  doesn't send any response
	//   1 -> WaitForLocal. waits for only the local commit to succeed before responding (default)
	//   -1 -> WaitForAll. waits for all in-sync replicas to commit before responding.
	RequiredAcks RequiredAcks `mapstructure:"required_acks"`

	// Compression Codec used to produce messages
	// https://pkg.go.dev/github.com/IBM/sarama@v1.30.0#CompressionCodec
	// The options are: 'none' (default), 'gzip', 'snappy', 'lz4', and 'zstd'
	Compression string `mapstructure:"compression"`

	// The maximum number of messages the producer will send in a single
	// broker request. Defaults to 0 for unlimited. Similar to
	// `queue.buffering.max.messages` in the JVM producer.
	FlushMaxMessages int `mapstructure:"flush_max_messages"`
}

func NewDefaultProducerConfig() ProducerConfig {
	return ProducerConfig{
		MaxMessageBytes:  1000000,
		RequiredAcks:     WaitForLocal,
		Compression:      "none",
		FlushMaxMessages: 0,
	}
}

func (c ProducerConfig) Validate() error {
	switch c.Compression {
	case "none", "gzip", "snappy", "lz4", "zstd":
		// Valid compression
	default:
		return fmt.Errorf(
			"compression should be one of 'none', 'gzip', 'snappy', 'lz4', or 'zstd'. configured value %v",
			c.Compression,
		)
	}
	return nil
}

// RequiredAcks defines record acknowledgement behavior for for producers.
//
// TODO change to a string, deprecate Sarama-specific magic numbers.
type RequiredAcks int

const (
	// NoResponse doesn't send any response, the TCP ACK is all you get.
	NoResponse RequiredAcks = 0
	// WaitForLocal waits for only the local commit to succeed before responding.
	WaitForLocal RequiredAcks = 1
	// WaitForAll waits for all in-sync replicas to commit before responding.
	// The minimum number of in-sync replicas is configured on the broker via
	// the `min.insync.replicas` configuration key.
	WaitForAll RequiredAcks = -1
)

func (r RequiredAcks) Validate() error {
	if r < -1 || r > 1 {
		return fmt.Errorf("required_acks must be between -1 and 1; configured value is %v", r)
	}
	return nil
}

type MetadataConfig struct {
	// Whether to maintain a full set of metadata for all topics, or just
	// the minimal set that has been necessary so far. The full set is simpler
	// and usually more convenient, but can take up a substantial amount of
	// memory if you have many topics and partitions. Defaults to true.
	Full bool `mapstructure:"full"`

	// Retry configuration for metadata.
	// This configuration is useful to avoid race conditions when broker
	// is starting at the same time as collector.
	Retry MetadataRetryConfig `mapstructure:"retry"`
}

// MetadataRetryConfig defines retry configuration for Metadata.
type MetadataRetryConfig struct {
	// The total number of times to retry a metadata request when the
	// cluster is in the middle of a leader election or at startup (default 3).
	Max int `mapstructure:"max"`
	// How long to wait for leader election to occur before retrying
	// (default 250ms). Similar to the JVM's `retry.backoff.ms`.
	Backoff time.Duration `mapstructure:"backoff"`
}

// AuthenticationConfig defines authentication-related configuration.
type AuthenticationConfig struct {
	PlainText *PlainTextConfig        `mapstructure:"plain_text"`
	SASL      *SASLConfig             `mapstructure:"sasl"`
	TLS       *configtls.ClientConfig `mapstructure:"tls"`
	Kerberos  *KerberosConfig         `mapstructure:"kerberos"`
}

// PlainTextConfig defines plaintext authentication.
type PlainTextConfig struct {
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}

// SASLConfig defines the configuration for the SASL authentication.
type SASLConfig struct {
	// Username to be used on authentication
	Username string `mapstructure:"username"`
	// Password to be used on authentication
	Password string `mapstructure:"password"`
	// SASL Mechanism to be used, possible values are: (PLAIN, AWS_MSK_IAM, AWS_MSK_IAM_OAUTHBEARER, SCRAM-SHA-256 or SCRAM-SHA-512).
	Mechanism string `mapstructure:"mechanism"`
	// SASL Protocol Version to be used, possible values are: (0, 1). Defaults to 0.
	Version int `mapstructure:"version"`
	// AWSMSK holds configuration specific to AWS MSK.
	AWSMSK AWSMSKConfig `mapstructure:"aws_msk"`
}

func (c SASLConfig) Validate() error {
	switch c.Mechanism {
	case "AWS_MSK_IAM", "AWS_MSK_IAM_OAUTHBEARER":
		// TODO validate c.AWSMSK
	case "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
		// Do nothing, valid mechanism
		if c.Username == "" {
			return fmt.Errorf("username is required")
		}
		if c.Password == "" {
			return fmt.Errorf("password is required")
		}
	default:
		return fmt.Errorf(
			"mechanism should be one of 'PLAIN', 'AWS_MSK_IAM', 'AWS_MSK_IAM_OAUTHBEARER', 'SCRAM-SHA-256' or 'SCRAM-SHA-512'. configured value %v",
			c.Mechanism,
		)
	}
	if c.Version < 0 || c.Version > 1 {
		return fmt.Errorf("version has to be either 0 or 1. configured value %v", c.Version)
	}
	return nil
}

// AWSMSKConfig defines the additional SASL authentication
// measures needed to use AWS_MSK_IAM and AWS_MSK_IAM_OAUTHBEARER mechanism
type AWSMSKConfig struct {
	// Region is the AWS region the MSK cluster is based in
	Region string `mapstructure:"region"`
	// BrokerAddr is the client is connecting to in order to perform the auth required
	BrokerAddr string `mapstructure:"broker_addr"`
}

// KerberosConfig defines kerberos configuration.
type KerberosConfig struct {
	ServiceName     string `mapstructure:"service_name"`
	Realm           string `mapstructure:"realm"`
	UseKeyTab       bool   `mapstructure:"use_keytab"`
	Username        string `mapstructure:"username"`
	Password        string `mapstructure:"password" json:"-"`
	ConfigPath      string `mapstructure:"config_file"`
	KeyTabPath      string `mapstructure:"keytab_file"`
	DisablePAFXFAST bool   `mapstructure:"disable_fast_negotiation"`
}
