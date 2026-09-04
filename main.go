// main.go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
)

var (
	cfg Config

	rootCmd = &cobra.Command{
		Use:   "cdc",
		Short: "PostgreSQL WAL Logical Replication & Change Data Capture Engine",
		Long:  `A lightweight Go CDC engine streaming PostgreSQL WAL output directly into message brokers.`,
	}

	startCmd = &cobra.Command{
		Use:   "start",
		Short: "Start streaming logical replication events",
		Run: func(cmd *cobra.Command, args []string) {
			_ = godotenv.Load()

			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			if err := RunEngine(ctx, cfg); err != nil {
				log.Fatalf("Fatal engine error: %v", err)
			}
		},
	}
)

func init() {
	// Flags for the "start" subcommand
	startCmd.Flags().StringVarP(&cfg.SlotName, "slot", "s", "go_cdc_slot", "PostgreSQL replication slot name")
	startCmd.Flags().StringVarP(&cfg.Publication, "pub", "p", "my_pub", "PostgreSQL publication name")
	startCmd.Flags().StringVar(&cfg.OutputPlugin, "plugin", "pgoutput", "PostgreSQL logical decoding output plugin")
	startCmd.Flags().StringVarP(&cfg.CheckpointFile, "checkpoint", "c", "checkpoint.lsn", "Path to checkpoint store file")
	startCmd.Flags().StringVarP(&cfg.BrokerType, "broker", "b", "log", "Target broker connector (log, kafka, rabbitmq, nats, pulsar)")
	startCmd.Flags().StringVar(&cfg.LogLevel, "log-level", "info", "Log output verbosity (debug, info, warn, error)")

	rootCmd.AddCommand(startCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
