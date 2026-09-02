package main

import (
	"brokers"
	"config"
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"streams"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/joho/godotenv"
)

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

const (
	SLOT_NAME       = "go_cdc_slot"
	OUTPUT_PLUGIN   = "pgoutput"
	Publication     = "my_pub"
	standBySeconds  = 10
	CHECKPOINT_FILE = "checkpoint.lsn"
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

func main() {

	InitMessage()

	ring := streams.NewRing(256) // preallocated fixed size

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	if err := godotenv.Load(); err != nil {
		log.Println("no .env file found, using system env")
	}

	logger.Info("replication started", "slot", "go_cdc_slot")

	ctx := context.Background()

	// load configuration

	// create broker config
	broker, err := brokers.NewFromEnv()
	if err != nil {
		log.Fatal("create broker: ", err)
	}

	connector := brokers.NewConnector("log", broker)
	defer connector.Close()

	// Load database credentials
	connStr, err := config.LoadDatabaseURL()

	if err != nil {
		log.Fatal(err)
	}

	// Connect to Postgres using the replication flag
	conn, err := pgconn.Connect(ctx, connStr)

	if err != nil {
		log.Fatal("connect", err)
	}

	defer conn.Close(ctx)

	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, SLOT_NAME, OUTPUT_PLUGIN, pglogrepl.CreateReplicationSlotOptions{})

	if err != nil {
		// ignore already exists
		log.Println("create slot: ", err)
	}

	store, err := streams.NewCheckpointStore(CHECKPOINT_FILE)

	if err != nil {
		log.Fatal(err)
	}

	defer store.Close()

	startLSN, err := store.Load()

	if err != nil {
		log.Fatal("load checkpoint: ", err)
	}

	if startLSN != 0 {
		log.Println("Resuming from LSN:", startLSN)
	} else {
		log.Println("No checkpoint found - starting from beginning")
	}

	// start streaming
	err = pglogrepl.StartReplication(ctx, conn, SLOT_NAME, startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			"proto_version '2'",
			"publication_names '" + Publication + "'",
		},
	})

	if err != nil {
		log.Fatal("StartReplication: ", err.Error())
	}

	log.Println("Logical replication started on slot: ", SLOT_NAME)

	// Create the async event bus
	bus := streams.NewEventBus(connector, 4, 1000, ring)
	defer bus.Close()

	fields := os.Getenv("PII_FIELDS")

	params := streams.ParsePIIFields(fields)

	var sanitizer *streams.PIISanitizer = &streams.PIISanitizer{
		Mode:   streams.PIIMode(os.Getenv("PII_MODE")),
		Fields: params,
		Salt:   "",
	}

	handler := streams.NewReplicationHandler(ctx, store, conn, startLSN, bus, sanitizer)
	if err := handler.Run(ctx); err != nil {
		log.Fatal(err)
	}

}
