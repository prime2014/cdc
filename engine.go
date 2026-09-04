package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"brokers"
	"config"
	"streams"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

type Config struct {
	SlotName       string
	Publication    string
	OutputPlugin   string
	CheckpointFile string
	BrokerType     string
	LogLevel       string
}

const (
	ColorReset  = "\033[0m"
	ColorRed    = "\033[31m"
	ColorGreen  = "\033[32m"
	ColorYellow = "\033[33m"
	ColorBlue   = "\033[34m"
	ColorPurple = "\033[35m"
	ColorCyan   = "\033[36m"
	ColorWhite  = "\033[37m"
)

func InitMessage() {
	fmt.Println()
	fmt.Println(ColorCyan + `  ┌─────────────────────────────────────────────────────────────┐` + ColorReset)
	fmt.Println(ColorCyan + `  │` + ColorPurple + `  ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓  ` + ColorCyan + `│` + ColorReset)
	fmt.Println(ColorCyan + `  │` + ColorYellow + `                                                             ` + ColorCyan + `│` + ColorReset)
	fmt.Println(ColorCyan + `  │` + ColorYellow + `     █ █ ▀█▀ █▀█ █▀▀ █▀▄▀█ █▀▀     █▀▀ █▀▄ █▀▀              ` + ColorCyan + `│` + ColorReset)
	fmt.Println(ColorCyan + `  │` + ColorYellow + `     █▄█  █  █▀▄ ██▄ █ ▀ █ ██▄     █▄▄ █▄▀ █▄▄              ` + ColorCyan + `│` + ColorReset)
	fmt.Println(ColorCyan + `  │` + ColorYellow + `                                                             ` + ColorCyan + `│` + ColorReset)
	fmt.Println(ColorCyan + `  │` + ColorGreen + `              ▄▀▀ ▄▀▄ ▀█▀ ▄▀█                               ` + ColorCyan + `│` + ColorReset)
	fmt.Println(ColorCyan + `  │` + ColorGreen + `              ▀▄▄ ▀▄▀  █  █▀█                               ` + ColorCyan + `│` + ColorReset)
	fmt.Println(ColorCyan + `  │` + ColorYellow + `                                                             ` + ColorCyan + `│` + ColorReset)
	fmt.Println(ColorCyan + `  │` + ColorWhite + `     ▸ logical replication  ·  change data capture          ` + ColorCyan + `│` + ColorReset)
	fmt.Println(ColorCyan + `  │` + ColorBlue + `     ▸ slot  ` + ColorWhite + `go_cdc_slot` + ColorBlue + `  ·  plugin  ` + ColorWhite + `pgoutput` + ColorCyan + `               │` + ColorReset)
	fmt.Println(ColorCyan + `  │` + ColorPurple + `  ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓  ` + ColorCyan + `│` + ColorReset)
	fmt.Println(ColorCyan + `  └─────────────────────────────────────────────────────────────┘` + ColorReset)
	fmt.Println()
}

func RunEngine(ctx context.Context, cfg Config) error {
	InitMessage()

	ring := streams.NewRing(256)

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLogLevel(cfg.LogLevel),
	}))

	logger.Info("starting logical replication", "slot", cfg.SlotName, "publication", cfg.Publication)

	broker, err := brokers.NewFromEnv()
	if err != nil {
		return fmt.Errorf("create broker: %w", err)
	}

	connector := brokers.NewConnector(cfg.BrokerType, broker)
	defer connector.Close()

	connStr, err := config.LoadDatabaseURL()
	if err != nil {
		return fmt.Errorf("load db url: %w", err)
	}

	conn, err := pgconn.Connect(ctx, connStr)
	if err != nil {
		return fmt.Errorf("postgres connect: %w", err)
	}
	defer conn.Close(ctx)

	// Ensure slot exists
	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, cfg.SlotName, cfg.OutputPlugin, pglogrepl.CreateReplicationSlotOptions{})
	if err != nil {
		logger.Debug("replication slot check/creation warning", "err", err)
	}

	store, err := streams.NewCheckpointStore(cfg.CheckpointFile)
	if err != nil {
		return fmt.Errorf("checkpoint store: %w", err)
	}
	defer store.Close()

	startLSN, err := store.Load()
	if err != nil {
		return fmt.Errorf("load checkpoint: %w", err)
	}

	if startLSN != 0 {
		logger.Info("resuming stream", "lsn", startLSN)
	} else {
		logger.Info("no checkpoint found, starting from 0")
	}

	err = pglogrepl.StartReplication(ctx, conn, cfg.SlotName, startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			"proto_version '2'",
			"publication_names '" + cfg.Publication + "'",
		},
	})
	if err != nil {
		return fmt.Errorf("start replication: %w", err)
	}

	bus := streams.NewEventBus(connector, 4, 1000, ring)
	defer bus.Close()

	sanitizer := &streams.PIISanitizer{
		Mode:   streams.PIIMode(os.Getenv("PII_MODE")),
		Fields: streams.ParsePIIFields(os.Getenv("PII_FIELDS")),
		Salt:   os.Getenv("PII_SALT"),
	}

	handler := streams.NewReplicationHandler(ctx, store, conn, startLSN, bus, sanitizer)
	return handler.Run(ctx)
}

func parseLogLevel(level string) slog.Level {
	switch level {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
