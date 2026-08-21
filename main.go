package main

import (
	"brokers"
	"config"
	"context"
	"log"
	"os"
	"streams"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	SLOT_NAME       = "go_cdc_slot"
	OUTPUT_PLUGIN   = "pgoutput"
	Publication     = "my_pub"
	standBySeconds  = 10
	CHECKPOINT_FILE = "checkpoint.lsn"
)

func main5() {
	ctx := context.Background()

	// load configuration
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatal("load config: ", err)
	}

	// create broker config
	broker, err := brokers.NewFromConfig(cfg.Broker)
	if err != nil {
		log.Fatal("create broker: ", err)
	}

	name := cfg.Broker.Name
	if name == "" {
		name = cfg.Broker.Type
	}

	connector := brokers.NewConnector(name, broker)
	defer connector.Close()

	connStr := os.Getenv("DATABASE_URL")

	if connStr == "" {
		log.Fatal("DATABASE_URL is required (must contain ?replication=database)")
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
			"publication_name '" + Publication + "'",
		},
	})

	if err != nil {
		log.Fatal("StartReplication: ", err.Error())
	}

	log.Println("Logical replication started on slot: ", SLOT_NAME)

	// Create the async event bus
	bus := streams.NewEventBus(connector, 4, 1000)
	defer bus.Close()

	handler := streams.NewReplicationHandler(store, conn, startLSN, bus)
	if err := handler.Run(ctx); err != nil {
		log.Fatal(err)
	}

}
