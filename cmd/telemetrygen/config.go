// Copyright The OpenTelemetry Authors
// Copyright (c) 2018 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

package main // import "github.com/open-telemetry/opentelemetry-collector-contrib/telemetrygen/internal/telemetrygen"

import (
	"context"
	_ "embed"
	"fmt"
	"log"
	"os"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"go.opentelemetry.io/contrib/config"

	"github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen/internal/logs"
	"github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen/internal/metrics"
	"github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen/internal/traces"
)

var (
	tracesCfg     traces.Config
	metricsCfg    metrics.Config
	logsCfg       logs.Config
	sdkConfigFile string

	//go:embed sdk-config.yaml
	defaultSDKConfigContent []byte

	sdkConfig *config.OpenTelemetryConfiguration
	sdk       config.SDK
	logger    *zap.Logger
)

// rootCmd is the root command on which will be run children commands
var rootCmd = &cobra.Command{
	Use:     "telemetrygen",
	Short:   "Telemetrygen simulates a client generating traces, metrics, and logs",
	Example: "telemetrygen traces\ntelemetrygen metrics\ntelemetrygen logs",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		// TODO init SDK; ensure one periodic metric reader is used,
		// and extract its interval for calculating how many metrics to producer per interval.
		return initSDK(cmd.Context())
	},
	PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
		return sdk.Shutdown(cmd.Context())
	},
}

func initSDK(ctx context.Context) error {
	// Default metrics interval to 1s.
	metricsExportInterval := os.Getenv("OTEL_METRIC_EXPORT_INTERVAL")
	if metricsExportInterval == "" {
		os.Setenv("OTEL_METRIC_EXPORT_INTERVAL", "1000")
	}

	var err error
	var sdkConfigFileContent []byte
	if sdkConfigFile != "" {
		sdkConfigFileContent, err = os.ReadFile(sdkConfigFile)
		if err != nil {
			return err
		}
	} else {
		sdkConfigFileContent = defaultSDKConfigContent
	}
	sdkConfig, err = config.ParseYAML(sdkConfigFileContent)
	if err != nil {
		return fmt.Errorf("failed to parse SDK config: %w", err)
	}

	s, err := config.NewSDK(
		config.WithContext(ctx),
		config.WithOpenTelemetryConfiguration(*sdkConfig),
	)
	if err != nil {
		return err
	}
	sdk = s
	return nil
}

// tracesCmd is the command responsible for sending traces
var tracesCmd = &cobra.Command{
	Use:     "traces",
	Short:   fmt.Sprintf("Simulates a client generating traces. (Stability level: %s)", metadata.TracesStability),
	Example: "telemetrygen traces",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()
		if d := tracesCfg.TotalDuration; d != 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, d)
			defer cancel()
		}
		return traces.Run(ctx, &tracesCfg, sdk.TracerProvider(), logger)
	},
}

// metricsCmd is the command responsible for sending metrics
var metricsCmd = &cobra.Command{
	Use:     "metrics",
	Short:   fmt.Sprintf("Simulates a client generating metrics. (Stability level: %s)", metadata.MetricsStability),
	Example: "telemetrygen metrics",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()
		if d := metricsCfg.TotalDuration; d != 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, d)
			defer cancel()
		}
		return metrics.Run(ctx, &metricsCfg, sdk.MeterProvider(), logger)
	},
}

// logsCmd is the command responsible for sending logs
var logsCmd = &cobra.Command{
	Use:     "logs",
	Short:   fmt.Sprintf("Simulates a client generating logs. (Stability level: %s)", metadata.LogsStability),
	Example: "telemetrygen logs",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()
		if d := logsCfg.TotalDuration; d != 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, d)
			defer cancel()
		}
		return logs.Run(ctx, &logsCfg, sdk.LoggerProvider(), logger)
	},
}

func init() {
	loggerConfig := zap.NewDevelopmentConfig()
	if level := os.Getenv("OTEL_LOG_LEVEL"); level != "" {
		logLevel, err := zapcore.ParseLevel(level)
		if err != nil {
			log.Fatal(err)
		}
		loggerConfig.Level.SetLevel(logLevel)
	}
	var err error
	logger, err = loggerConfig.Build()
	if err != nil {
		log.Fatal(err)
	}

	rootCmd.AddCommand(tracesCmd, metricsCmd, logsCmd)
	rootCmd.PersistentFlags().StringVarP(
		&sdkConfigFile, "config", "c",
		os.Getenv("OTEL_CONFIG_FILE"),
		"OpenTelemetry SDK configuration file",
	)

	tracesCfg.Flags(tracesCmd.Flags())
	metricsCfg.Flags(metricsCmd.Flags())
	logsCfg.Flags(logsCmd.Flags())

	// Disabling completion command for end user
	// https://github.com/spf13/cobra/blob/master/shell_completions.md
	rootCmd.CompletionOptions.DisableDefaultCmd = true
}

// Execute tries to run the input command
func Execute() {
	if err := rootCmd.ExecuteContext(context.Background()); err != nil {
		// TODO: Uncomment the line below when using Run instead of RunE in the xxxCmd functions
		// fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
