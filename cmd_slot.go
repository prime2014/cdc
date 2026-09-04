// cmd_slot.go
package main

import (
	"context"
	"fmt"
	"log"

	"config"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/spf13/cobra"
)

var slotCmd = &cobra.Command{
	Use:   "slot",
	Short: "Manage PostgreSQL logical replication slots",
}

var dropSlotCmd = &cobra.Command{
	Use:   "drop [slot_name]",
	Short: "Drop a logical replication slot on the database",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		slotName := args[0]
		ctx := context.Background()

		connStr, err := config.LoadDatabaseURL()
		if err != nil {
			log.Fatalf("Failed to load DB credentials: %v", err)
		}

		conn, err := pgconn.Connect(ctx, connStr)
		if err != nil {
			log.Fatalf("Connection failed: %v", err)
		}
		defer conn.Close(ctx)

		err = pglogrepl.DropReplicationSlot(ctx, conn, slotName, pglogrepl.DropReplicationSlotOptions{Wait: true})
		if err != nil {
			log.Fatalf("Failed to drop slot %s: %v", slotName, err)
		}

		fmt.Printf("Successfully dropped replication slot: %s\n", slotName)
	},
}

func init() {
	slotCmd.AddCommand(dropSlotCmd)
	rootCmd.AddCommand(slotCmd)
}
