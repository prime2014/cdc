# INTRODUCTION

## TABLE OF CONTENT
- [Description](#description)
- [High Level Architecture](#high-level-architecture)
- [Detailed data flow](#detailed-data-flow)
- [Go CDC – PostgreSQL Change Data Capture](#go-cdc--postgresql-change-data-capture)
- [Project layout](#project-layout)
- [Prerequisites](#prerequisites)
- [PostgreSQL setup](#postgresql-setup)
- [Configuration](#configuration)
- [CDC Event format](#cdc-event-format)
- [Development](#development)
- [License](#license)

### Description
What is CDC ```(Change Data Capture)``` tool?
```
    A change data capture tool captures changes that occur in data storage systems, mainly databases and uses these changes to sync downstream systems that need this data, e.g
    Elasticserch, redis, analytics engine, data warehouse
```

### High Level Architecture

```mermaid
flowchart LR
    subgraph Source
        PG[(PostgreSQL)]
    end

    subgraph CDC["Go CDC Service"]
        RH[ReplicationHandler]
        CP[Checkpoint Store]
        EB[Event Bus<br/>workers + sync.Pool]
        CON[Connector]
    end

    subgraph Brokers
        K[Kafka / Redpanda]
        N[NATS]
        R[RabbitMQ]
        M[MQTT]
        P[Pulsar]
        L[LogBroker]
    end

    PG -->|Logical Replication<br/>pgoutput + LSN| RH
    RH --> CP
    RH --> EB
    EB --> CON
    CON --> K & N & R & M & P & L

```


### Detailed data flow

```mermaid
sequenceDiagram
    participant PG as PostgreSQL
    participant RH as ReplicationHandler
    participant CP as Checkpoint Store
    participant EB as Event Bus
    participant BR as Broker

    PG->>RH: XLogData (Insert/Update/Delete)
    RH->>RH: Parse → CDCEvent
    RH->>EB: Submit(event)  (non-blocking)
    RH->>CP: Save(LSN)

    EB->>BR: Publish(event)
    BR-->>EB: ack / error
    EB->>EB: putEvent back to pool
```


# Go CDC – PostgreSQL Change Data Capture

A lightweight, production-oriented Change Data Capture (CDC) service written in Go.  
It streams row-level changes from PostgreSQL using logical replication and publishes them to one of several message brokers.

---

## Features

- PostgreSQL logical replication (`pgoutput`)
- LSN-based checkpointing (resume after restart)
- Pluggable brokers: Kafka, Redpanda, NATS, RabbitMQ, MQTT, Apache Pulsar, Log
- Async event bus with worker pool (replication loop never blocks)
- Optional `sync.Pool` for reduced allocations
- YAML-driven configuration
- Clean interface-based design (`Broker`, `Connector`)

---

Data flow (simplified)

- PostgreSQL sends WAL changes via a replication slot.
- ReplicationHandler decodes Insert / Update / Delete messages into CDCEvent.
- The LSN is persisted to a local checkpoint file.
- Events are handed to an asynchronous EventBus.
- Worker goroutines publish events through a Connector to the configured broker.

---
### Project Layout

```
C:.
│   .gitignore
│   go.mod
│   go.sum
│   go.work
│   go.work.sum
│   heartbeat.go
│   main.go
│   README.md
│   
└───src
    │   go.mod
    │   
    ├───brokers
    │       config.go
    │       connector.go
    │       go.mod
    │       kafka.go
    │       log_broker.go
    │       mqtt.go
    │       nats.go
    │       pulsar.go
    │       rabbitmq.go
    │       
    ├───config
    │       config.go
    │       go.mod
    │       
    └───streams
            cdc.go
            event_bus.go
            fileOperations.go
            go.mod
            handler.go
```

### Prerequisites

- Go 1.22+
- PostgreSQL 12+ with logical replication enabled
- A publication and a table with `REPLICA IDENTITY` FULL (or at least a primary key)


### PostgreSQL setup
```
-- postgresql.conf
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10

-- Create publication
CREATE PUBLICATION my_pub FOR TABLE your_table;

-- Optional but recommended for UPDATE/DELETE old values
ALTER TABLE your_table REPLICA IDENTITY FULL;
```

Connection string must contain the replication flag:

```
postgres://user:pass@host:5432/dbname?replication=database
```


### Configuration

`.env` example:

```
# PostgreSQL
DATABASE_URL=postgres://user:password@localhost:5432/database

# Broker selection
# Ech broker selection goes hand-in-hand with the broker type examples below
BROKER_TYPE=kafka          # kafka|redpanda|nats|rabbitmq|mqtt|pulsar|log
BROKER_NAME=main

# Kafka / Redpanda
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC=cdc-events

# NATS
NATS_URL=nats://localhost:4222
NATS_SUBJECT=cdc.events

# RabbitMQ
RABBITMQ_URL=amqp://guest:guest@localhost:5672/
RABBITMQ_EXCHANGE=cdc     # set this as topic exchange with routing key: cdc.#

# MQTT
MQTT_URL=tcp://localhost:1883
MQTT_CLIENT_ID=cdc-publisher
MQTT_TOPIC=cdc

# Pulsar
PULSAR_URL=pulsar://localhost:6650
PULSAR_TOPIC=persistent://public/default/cdc-events
```

Other broker examples are documented in the `brokers` package / comments.

---

### Running

```
export DATABASE_URL="postgres://user:pass@localhost:5432/mydb?replication=database"

go run .
```

On first start the service creates the replication slot `go_cdc_slot` (if it does not exist) and begins streaming from LSN `0/0`.
Subsequent starts resume from the LSN stored in `checkpoint.lsn`.


### CDC Event format

```
{
  "id": "1712345678901",
  "schema": "public",
  "table": "users",
  "op": "INSERT",
  "timestamp": 1712345678901,
  "before": null,
  "after": {
    "id": 42,
    "name": "Alice"
  }
}
```

`op` is one of `INSERT`, `UPDATE`, `DELETE`.

---
### Checkpoint & delivery semantics

- The service tracks the PostgreSQL LSN.
- After a change is successfully handed to the event bus the LSN is written to disk.
- Delivery to the broker is at-least-once.
After a crash you may see a small number of duplicate events; consumers should be idempotent.

---

## Extending
### Add a new broker

1. Implement the `brokers.Broker` interface.
2. Add a case in `brokers.NewFromConfig`.
3. Document the required `options` keys.

### Change the event schema
Edit `streams.CDCEvent` and the decoding helpers inside `ReplicationHandler`.

---

## Development

```
go mod tidy
go test ./...
go run .
```

For local testing without a real broker set:
```
.env
BROKER_TYPE=log
BROKER_NAME=local-test
```


## License
MIT