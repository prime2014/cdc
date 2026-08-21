package streams

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

type Broker interface {
	Publish(ctx context.Context, event *CDCEvent) error
}

type ReplicationHandler struct {
	// 1. Standard 64-bit Pointers, Maps & Primitive Types (8 bytes each)
	store          *CheckpointStore                      // 8 bytes
	conn           *pgconn.PgConn                        // 8 bytes
	clientXLogPos  pglogrepl.LSN                         // 8 bytes (uint64)
	relations      map[uint32]*pglogrepl.RelationMessage // 8 bytes
	typeMap        *pgtype.Map                           // 8 bytes
	bus            *EventBus                             // 8 bytes
	standbyTimeout time.Duration                         // 8 bytes (int64)

	// 2. Struct Value Types (24 bytes)
	nextStandbyDeadline time.Time // 24 bytes (wall uint64 + ext int64 + loc ptr)
}

func NewReplicationHandler(
	store *CheckpointStore,
	conn *pgconn.PgConn,
	startLSN pglogrepl.LSN,
	bus *EventBus,
) *ReplicationHandler {
	h := &ReplicationHandler{
		store:               store,
		conn:                conn,
		clientXLogPos:       startLSN,
		relations:           make(map[uint32]*pglogrepl.RelationMessage),
		standbyTimeout:      10 * time.Second,
		nextStandbyDeadline: time.Now().Add(10 * time.Second),

		bus: bus,
	}

	return h
}

func (h *ReplicationHandler) Run(ctx context.Context) error {
	for {
		if time.Now().After(h.nextStandbyDeadline) {
			if err := h.sendStandbyStatus(ctx); err != nil {
				return err
			}
			h.nextStandbyDeadline = time.Now().Add(h.standbyTimeout)
		}

		receiveCtx, cancel := context.WithDeadline(ctx, h.nextStandbyDeadline)
		msg, err := h.conn.ReceiveMessage(receiveCtx)
		cancel()

		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return err
		}

		if err := h.handleMessage(msg); err != nil {
			return err
		}
	}
}

func (h *ReplicationHandler) handleMessage(msg pgproto3.BackendMessage) error {
	switch msg := msg.(type) {
	case *pgproto3.CopyData:
		return h.handleCopyData(msg)
	default:
		log.Printf("Unexpected message type: %T", msg)
		return nil
	}
}

func (h *ReplicationHandler) handleCopyData(msg *pgproto3.CopyData) error {
	switch msg.Data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		return h.handleKeepalive(msg.Data[1:])
	case pglogrepl.XLogDataByteID:
		return h.handleXLogData(msg.Data[1:])
	default:
		return nil
	}
}

func (h *ReplicationHandler) handleKeepalive(data []byte) error {
	pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(data)
	if err != nil {
		return err
	}

	if pkm.ReplyRequested {
		h.nextStandbyDeadline = time.Time{}
	}

	return nil
}

func (h *ReplicationHandler) handleXLogData(data []byte) error {
	xld, err := pglogrepl.ParseXLogData(data)
	if err != nil {
		return err
	}

	logicalMsg, err := pglogrepl.Parse(xld.WALData)

	if err != nil {
		log.Printf("Parse logical message: %v", err)
		return nil
	}

	if err := h.handleLogicalMessage(logicalMsg); err != nil {
		return err
	}

	// Advance position after successful processing
	h.clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))

	// Persist it
	if err := h.store.Save(h.clientXLogPos); err != nil {
		log.Printf("failed to save checkpoint: %v", err)
	}

	return nil
}

func (h *ReplicationHandler) handleLogicalMessage(msg pglogrepl.Message) error {
	switch m := msg.(type) {
	case *pglogrepl.RelationMessage:
		return h.handleRelation(m)
	case *pglogrepl.InsertMessage:
		return h.handleInsert(m)
	case *pglogrepl.UpdateMessage:
		return h.handleUpdate(m)
	case *pglogrepl.DeleteMessage:
		return h.handleDelete(m)
	case *pglogrepl.BeginMessage, *pglogrepl.CommitMessage:
		return nil
	default:
		return nil
	}
}

func (h *ReplicationHandler) handleRelation(m *pglogrepl.RelationMessage) error {
	h.relations[m.RelationID] = m
	log.Printf("Relation: %s.%s (id=%d)", m.Namespace, m.RelationName, m.RelationID)
	return nil
}

func (h *ReplicationHandler) handleInsert(m *pglogrepl.InsertMessage) error {
	rel, ok := h.relations[m.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation id: %d", m.RelationID)
	}
	log.Printf("INSERT into %s.%s", rel.Namespace, rel.RelationName)
	// TODO: build CDCEvent
	return nil
}

func (h *ReplicationHandler) handleUpdate(m *pglogrepl.UpdateMessage) error {
	// similar
	return nil
}

func (h *ReplicationHandler) handleDelete(m *pglogrepl.DeleteMessage) error {
	// similar
	return nil
}

func (h *ReplicationHandler) sendStandbyStatus(ctx context.Context) error {
	return pglogrepl.SendStandbyStatusUpdate(ctx, h.conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: h.clientXLogPos,
		WALFlushPosition: h.clientXLogPos,
		WALApplyPosition: h.clientXLogPos,
	})
}
