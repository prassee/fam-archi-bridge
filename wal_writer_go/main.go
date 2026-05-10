package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
)

type appConfig struct {
	pgHost      string
	pgPort      string
	pgUser      string
	pgPassword  string
	pgDatabase  string
	pgSlotName  string
	publication string
	kafkaBroker string
	topicPrefix string
	acks        string
	lingerMs    int
	batchSize   int
	pollMs      int
}

type relationColumn struct {
	Name    string
	TypeOID uint32
}

type relationMeta struct {
	ID              uint32
	Schema          string
	Table           string
	ReplicaIdentity byte
	Columns         []relationColumn
}

type txnState struct {
	XID        uint32
	CommitTime int64
	LSN        uint64
}

type column struct {
	Name   string `json:"name"`
	TypeID uint32 `json:"type_oid"`
	Value  any    `json:"value"`
	IsNull bool   `json:"is_null"`
}

type rowData struct {
	Columns []column `json:"columns"`
}

type walRecord struct {
	LSN             uint64   `json:"lsn"`
	TableSchema     string   `json:"table_schema"`
	TableName       string   `json:"table_name"`
	Operation       string   `json:"operation"`
	OID             uint32   `json:"oid"`
	NewTuple        *rowData `json:"new_tuple"`
	OldTuple        *rowData `json:"old_tuple"`
	PartialOldTuple bool     `json:"partial_old_tuple,omitempty"`
	TxCommitTime    int64    `json:"tx_commit_time"`
	TxXID           uint32   `json:"tx_xid"`

	Topic   string `json:"-"`
	Key     []byte `json:"-"`
	Payload []byte `json:"-"`
}

func (r *walRecord) prepareMessage(prefix string) error {
	r.Topic = prefix + "." + r.TableSchema + "." + r.TableName
	if len(r.Key) == 0 {
		r.Key = []byte(strconv.FormatUint(uint64(r.TxXID), 10))
	}
	if len(r.Payload) == 0 {
		payload, err := json.Marshal(r)
		if err != nil {
			return err
		}
		r.Payload = payload
	}
	return nil
}

type cdcState struct {
	relations map[uint32]relationMeta
	txn       txnState
}

type runtime struct {
	logger           *log.Logger
	cfg              appConfig
	ready            atomic.Bool
	writer           *kafka.Writer
	state            cdcState
	stats            writerStats
	recordQueue      chan walRecord
	topicQueueMu     sync.Mutex
	topicQueues      map[string]chan walRecord
	pendingMu        sync.Mutex
	pendingLSNCounts map[uint64]int
	confirmedLSN     atomic.Uint64
	m                *walMetrics
}

type writerStats struct {
	published atomic.Uint64
	failed    atomic.Uint64
}

type walMetrics struct {
	ready                  prometheus.Gauge
	replicationConnected   prometheus.Gauge
	lastReceiveLSN         prometheus.Gauge
	lastProcessLSN         prometheus.Gauge
	walMessages            *prometheus.CounterVec
	publishedRecords       *prometheus.CounterVec
	publishFailures        *prometheus.CounterVec
	publishDurationSeconds prometheus.Histogram
	replicationLoopExits   prometheus.Counter
	processWALErrors       prometheus.Counter
	slotResets             prometheus.Counter
}

func newWalMetrics() *walMetrics {
	m := &walMetrics{
		ready: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "wal_writer_ready",
			Help: "Readiness state of wal-writer-go (1=ready, 0=not ready)",
		}),
		replicationConnected: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "wal_writer_replication_connected",
			Help: "Replication connection state (1=connected, 0=disconnected)",
		}),
		lastReceiveLSN: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "wal_writer_last_receive_lsn",
			Help: "Last WAL LSN received from PostgreSQL",
		}),
		lastProcessLSN: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "wal_writer_last_process_lsn",
			Help: "Last WAL LSN processed by wal-writer-go",
		}),
		walMessages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wal_writer_wal_messages_total",
			Help: "Number of parsed WAL protocol messages by type",
		}, []string{"type"}),
		publishedRecords: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wal_writer_published_records_total",
			Help: "Number of CDC records published to Kafka",
		}, []string{"operation", "topic"}),
		publishFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wal_writer_publish_failures_total",
			Help: "Number of CDC publish failures",
		}, []string{"operation", "topic"}),
		publishDurationSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "wal_writer_publish_duration_seconds",
			Help:    "End-to-end latency of Kafka publish attempts",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
		}),
		replicationLoopExits: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wal_writer_replication_loop_exits_total",
			Help: "Count of replication loop exits",
		}),
		processWALErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wal_writer_process_wal_errors_total",
			Help: "Count of WAL processing errors",
		}),
		slotResets: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wal_writer_slot_resets_total",
			Help: "Count of replication slot reset operations after slot-loss errors",
		}),
	}

	prometheus.MustRegister(
		m.ready,
		m.replicationConnected,
		m.lastReceiveLSN,
		m.lastProcessLSN,
		m.walMessages,
		m.publishedRecords,
		m.publishFailures,
		m.publishDurationSeconds,
		m.replicationLoopExits,
		m.processWALErrors,
		m.slotResets,
	)

	return m
}

func readEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func readEnvInt(key string, fallback int) int {
	val := strings.TrimSpace(readEnv(key, ""))
	if val == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(val)
	if err != nil {
		return fallback
	}
	return parsed
}

func loadConfig() appConfig {
	return appConfig{
		pgHost:      readEnv("WAL_WRITER_PG_HOST", "localhost"),
		pgPort:      readEnv("WAL_WRITER_PG_PORT", "5432"),
		pgUser:      readEnv("WAL_WRITER_PG_USER", "postgres"),
		pgPassword:  readEnv("WAL_WRITER_PG_PASSWORD", ""),
		pgDatabase:  readEnv("WAL_WRITER_PG_DATABASE", "postgres"),
		pgSlotName:  readEnv("WAL_WRITER_PG_SLOT_NAME", "wal_writer_slot"),
		publication: readEnv("WAL_WRITER_PUBLICATION", "wal_writer_publication"),
		kafkaBroker: readEnv("WAL_WRITER_KAFKA_BROKERS", "localhost:9092"),
		topicPrefix: readEnv("WAL_WRITER_KAFKA_TOPIC_PREFIX", "cdc"),
		acks:        readEnv("WAL_WRITER_KAFKA_ACKS", "all"),
		lingerMs:    readEnvInt("WAL_WRITER_KAFKA_LINGER_MS", 5),
		batchSize:   readEnvInt("WAL_WRITER_KAFKA_BATCH_SIZE", 16384),
		pollMs:      readEnvInt("WAL_WRITER_REPLICATION_POLL_INTERVAL_MS", 100),
	}
}

func healthMux(ready *atomic.Bool) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("not-ready"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}

func newRuntime(cfg appConfig, logger *log.Logger) *runtime {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(strings.Split(cfg.kafkaBroker, ",")...),
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		// Use configured linger so the broker can coalesce messages, but keep
		// publisher-side batching as well. This avoids overly small writes at high
		// event rates.
		BatchTimeout:           time.Duration(cfg.lingerMs) * time.Millisecond,
		BatchSize:              cfg.batchSize,
		AllowAutoTopicCreation: true,
	}
	if strings.EqualFold(cfg.acks, "1") {
		writer.RequiredAcks = kafka.RequireOne
	}
	if strings.EqualFold(cfg.acks, "0") {
		writer.RequiredAcks = kafka.RequireNone
	}

	rt := &runtime{
		logger: logger,
		cfg:    cfg,
		writer: writer,
		m:      newWalMetrics(),
		state: cdcState{
			relations: map[uint32]relationMeta{},
		},
		recordQueue:      make(chan walRecord, cfg.batchSize*4),
		topicQueues:      make(map[string]chan walRecord),
		pendingLSNCounts: make(map[uint64]int),
	}
	rt.m.ready.Set(0)
	rt.m.replicationConnected.Set(0)
	return rt
}

func main() {
	cfg := loadConfig()
	logger := log.New(os.Stdout, "", log.LstdFlags|log.LUTC)
	logger.Printf("starting wal-writer (go): pg=%s:%s db=%s slot=%s kafka=%s publication=%s topic_prefix=%s",
		cfg.pgHost, cfg.pgPort, cfg.pgDatabase, cfg.pgSlotName, cfg.kafkaBroker, cfg.publication, cfg.topicPrefix)

	rt := newRuntime(cfg, logger)

	server := &http.Server{
		Addr:              ":9090",
		Handler:           healthMux(&rt.ready),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatalf("health server failed: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- rt.runReplication(ctx)
	}()

	go rt.logStats(ctx)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-stop:
		logger.Printf("received shutdown signal: %s", sig)
		cancel()
	case err := <-errCh:
		if err != nil {
			logger.Printf("replication loop exited with error: %v", redactPassword(err, cfg.pgPassword))
		}
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = server.Shutdown(shutdownCtx)
	_ = rt.writer.Close()
	logger.Println("wal-writer (go) stopped")
}

func (rt *runtime) runReplication(ctx context.Context) error {
	defer rt.m.replicationLoopExits.Inc()
	go rt.publisher(ctx)

	connStr := "host=" + rt.cfg.pgHost +
		" port=" + rt.cfg.pgPort +
		" user=" + rt.cfg.pgUser +
		" password=" + rt.cfg.pgPassword +
		" dbname=" + rt.cfg.pgDatabase +
		" replication=database"

outer:
	for {
		if ctx.Err() != nil {
			return nil
		}

		rt.m.replicationConnected.Set(0)
		rt.ready.Store(false)
		rt.m.ready.Set(0)

		conn, err := pgconn.Connect(ctx, connStr)
		if err != nil {
			return redactPassword(err, rt.cfg.pgPassword)
		}

		sysident, err := pglogrepl.IdentifySystem(ctx, conn)
		if err != nil {
			conn.Close(context.Background())
			return err
		}
		rt.m.replicationConnected.Set(1)
		rt.logger.Printf("replication connected: systemid=%s timeline=%d xlogpos=%s db=%s", sysident.SystemID, sysident.Timeline, sysident.XLogPos.String(), sysident.DBName)

		pluginArgs := []string{
			"proto_version '1'",
			"publication_names '" + rt.cfg.publication + "'",
		}
		if err := pglogrepl.StartReplication(ctx, conn, rt.cfg.pgSlotName, pglogrepl.LSN(0), pglogrepl.StartReplicationOptions{PluginArgs: pluginArgs}); err != nil {
			if slotLostError(err) {
				rt.logger.Printf("replication slot appears lost, attempting recreation: %v", err)
				if resetErr := rt.resetReplicationSlot(ctx); resetErr != nil {
					conn.Close(context.Background())
					rt.m.replicationConnected.Set(0)
					return errors.Join(err, resetErr)
				}
				conn.Close(context.Background())
				rt.m.replicationConnected.Set(0)
				continue outer
			}
			conn.Close(context.Background())
			rt.m.replicationConnected.Set(0)
			return err
		}
		rt.ready.Store(true)
		rt.m.ready.Set(1)
		rt.logger.Printf("started logical replication slot=%s publication=%s", rt.cfg.pgSlotName, rt.cfg.publication)

		standbyTimeout := 2 * time.Second
		nextStatus := time.Now().Add(standbyTimeout)
		var lastReceiveLSN pglogrepl.LSN

		for {
			if ctx.Err() != nil {
				conn.Close(context.Background())
				rt.m.replicationConnected.Set(0)
				return nil
			}

			if time.Now().After(nextStatus) {
				confirmedLSN := pglogrepl.LSN(rt.confirmedLSN.Load())
				err = pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
					WALWritePosition: lastReceiveLSN,
					WALFlushPosition: confirmedLSN,
					WALApplyPosition: confirmedLSN,
				})
				if err != nil {
					conn.Close(context.Background())
					return err
				}
				nextStatus = time.Now().Add(standbyTimeout)
			}

			recvCtx, cancelRecv := context.WithTimeout(ctx, time.Duration(rt.cfg.pollMs)*time.Millisecond)
			msg, recvErr := conn.ReceiveMessage(recvCtx)
			cancelRecv()

			if recvErr != nil {
				if pgconn.Timeout(recvErr) || errors.Is(recvErr, context.DeadlineExceeded) || errors.Is(recvErr, context.Canceled) {
					continue
				}
				if slotLostError(recvErr) {
					rt.logger.Printf("replication slot lost during receive, resetting slot")
					if resetErr := rt.resetReplicationSlot(ctx); resetErr != nil {
						conn.Close(context.Background())
						rt.m.replicationConnected.Set(0)
						return errors.Join(recvErr, resetErr)
					}
					conn.Close(context.Background())
					continue outer
				}
				conn.Close(context.Background())
				rt.m.replicationConnected.Set(0)
				return recvErr
			}

			copyData, ok := msg.(*pgproto3.CopyData)
			if !ok || len(copyData.Data) == 0 {
				continue
			}

			switch copyData.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				rt.m.walMessages.WithLabelValues("keepalive").Inc()
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
				if err != nil {
					conn.Close(context.Background())
					return err
				}
				if pkm.ServerWALEnd > lastReceiveLSN {
					lastReceiveLSN = pkm.ServerWALEnd
					rt.m.lastReceiveLSN.Set(float64(lastReceiveLSN))
				}
				if pkm.ReplyRequested {
					nextStatus = time.Time{}
				}
			case pglogrepl.XLogDataByteID:
				rt.m.walMessages.WithLabelValues("xlogdata").Inc()
				xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
				if err != nil {
					conn.Close(context.Background())
					return err
				}
				if xld.ServerWALEnd > lastReceiveLSN {
					lastReceiveLSN = xld.ServerWALEnd
					rt.m.lastReceiveLSN.Set(float64(lastReceiveLSN))
				}
				rt.m.lastProcessLSN.Set(float64(xld.WALStart))
				if err := rt.processWALData(ctx, xld.WALData, uint64(xld.WALStart)); err != nil {
					rt.m.processWALErrors.Inc()
					rt.logger.Printf("process WAL data error: %v", err)
					continue
				}
				// Do NOT force immediate StandbyStatusUpdate here: confirmedLSN is
				// updated asynchronously by the publisher goroutine after Kafka
				// delivery.  Sending WALFlushPosition=0 (before the first publish
				// confirms) keeps confirmed_flush_lsn NULL in the slot and causes
				// PostgreSQL to retain all WAL until the slot is invalidated.
				// The periodic standbyTimeout heartbeat (plus ReplyRequested keepalives)
				// is sufficient to keep the connection alive.
			}
		}
	}
}

func (rt *runtime) processWALData(ctx context.Context, wal []byte, lsn uint64) error {
	buf := wal
	for len(buf) > 0 {
		msgType := buf[0]
		buf = buf[1:]
		rt.m.walMessages.WithLabelValues(walMessageType(msgType)).Inc()

		switch msgType {
		case 'B':
			if len(buf) < 20 {
				return errors.New("short BEGIN message")
			}
			rt.state.txn.LSN = binary.BigEndian.Uint64(buf[0:8])
			rt.state.txn.CommitTime = int64(binary.BigEndian.Uint64(buf[8:16]))
			rt.state.txn.XID = binary.BigEndian.Uint32(buf[16:20])
			buf = buf[20:]
		case 'C':
			if len(buf) < 25 {
				return errors.New("short COMMIT message")
			}
			rt.state.txn.LSN = binary.BigEndian.Uint64(buf[1:9])
			rt.state.txn.CommitTime = int64(binary.BigEndian.Uint64(buf[17:25]))
			buf = buf[25:]
		case 'R':
			next, rel, err := parseRelation(buf)
			if err != nil {
				return err
			}
			rt.state.relations[rel.ID] = rel
			buf = next
		case 'I':
			next, record, err := rt.parseInsert(buf, lsn)
			if err != nil {
				return err
			}
			buf = next
			if err := rt.publishRecord(ctx, record); err != nil {
				rt.logger.Printf("publish INSERT failed: %v", err)
			}
		case 'U':
			next, record, err := rt.parseUpdate(buf, lsn)
			if err != nil {
				return err
			}
			buf = next
			if err := rt.publishRecord(ctx, record); err != nil {
				rt.logger.Printf("publish UPDATE failed: %v", err)
			}
		case 'D':
			next, record, err := rt.parseDelete(buf, lsn)
			if err != nil {
				return err
			}
			buf = next
			if err := rt.publishRecord(ctx, record); err != nil {
				rt.logger.Printf("publish DELETE failed: %v", err)
			}
		case 'T':
			next, records, err := rt.parseTruncate(buf, lsn)
			if err != nil {
				return err
			}
			buf = next
			for _, rec := range records {
				if err := rt.publishRecord(ctx, rec); err != nil {
					rt.logger.Printf("publish TRUNCATE failed: %v", err)
				}
			}
		case 'O':
			// Origin message: commit_lsn + origin name.
			if len(buf) < 8 {
				return errors.New("short ORIGIN message")
			}
			_, rem, ok := parseCString(buf[8:])
			if !ok {
				return errors.New("invalid ORIGIN cstring")
			}
			buf = rem
		default:
			// Skip unsupported message types to keep stream alive.
			return nil
		}
	}
	return nil
}

func parseRelation(buf []byte) ([]byte, relationMeta, error) {
	if len(buf) < 7 {
		return nil, relationMeta{}, errors.New("short RELATION header")
	}
	relID := binary.BigEndian.Uint32(buf[0:4])
	rem := buf[4:]

	schema, rem2, ok := parseCString(rem)
	if !ok {
		return nil, relationMeta{}, errors.New("invalid RELATION schema")
	}
	table, rem3, ok := parseCString(rem2)
	if !ok {
		return nil, relationMeta{}, errors.New("invalid RELATION table")
	}
	if len(rem3) < 3 {
		return nil, relationMeta{}, errors.New("short RELATION column header")
	}
	replicaIdentity := rem3[0]
	rem3 = rem3[1:]
	colCount := int(binary.BigEndian.Uint16(rem3[0:2]))
	rem3 = rem3[2:]

	cols := make([]relationColumn, 0, colCount)
	for i := 0; i < colCount; i++ {
		if len(rem3) < 1 {
			return nil, relationMeta{}, errors.New("short RELATION column flags")
		}
		rem3 = rem3[1:] // flags
		colName, rem4, ok := parseCString(rem3)
		if !ok {
			return nil, relationMeta{}, errors.New("invalid RELATION column name")
		}
		if len(rem4) < 8 {
			return nil, relationMeta{}, errors.New("short RELATION column type")
		}
		typeOID := binary.BigEndian.Uint32(rem4[0:4])
		rem3 = rem4[8:] // oid + modifier
		cols = append(cols, relationColumn{Name: colName, TypeOID: typeOID})
	}

	return rem3, relationMeta{ID: relID, Schema: schema, Table: table, ReplicaIdentity: replicaIdentity, Columns: cols}, nil
}

func walMessageType(msgType byte) string {
	switch msgType {
	case 'B':
		return "begin"
	case 'C':
		return "commit"
	case 'R':
		return "relation"
	case 'I':
		return "insert"
	case 'U':
		return "update"
	case 'D':
		return "delete"
	case 'T':
		return "truncate"
	case 'O':
		return "origin"
	default:
		return "unknown"
	}
}

func (rt *runtime) parseInsert(buf []byte, lsn uint64) ([]byte, walRecord, error) {
	if len(buf) < 5 {
		return nil, walRecord{}, errors.New("short INSERT")
	}
	relID := binary.BigEndian.Uint32(buf[0:4])
	tag := buf[4]
	if tag != 'N' {
		return nil, walRecord{}, errors.New("invalid INSERT tuple tag")
	}
	rem, row, err := rt.parseTuple(buf[5:], relID)
	if err != nil {
		return nil, walRecord{}, err
	}
	rel := rt.lookupRelation(relID)
	record := walRecord{
		LSN:          lsn,
		TableSchema:  rel.Schema,
		TableName:    rel.Table,
		Operation:    "Insert",
		OID:          relID,
		NewTuple:     row,
		OldTuple:     nil,
		TxCommitTime: rt.state.txn.CommitTime,
		TxXID:        rt.state.txn.XID,
	}
	if err := record.prepareMessage(rt.cfg.topicPrefix); err != nil {
		return nil, walRecord{}, err
	}
	return rem, record, nil
}

func (rt *runtime) parseUpdate(buf []byte, lsn uint64) ([]byte, walRecord, error) {
	if len(buf) < 5 {
		return nil, walRecord{}, errors.New("short UPDATE")
	}
	relID := binary.BigEndian.Uint32(buf[0:4])
	rem := buf[4:]

	var oldRow *rowData
	for {
		if len(rem) < 1 {
			return nil, walRecord{}, errors.New("short UPDATE tuple tag")
		}
		tag := rem[0]
		rem = rem[1:]

		if tag == 'N' {
			break
		}

		if tag != 'K' && tag != 'O' {
			return nil, walRecord{}, fmt.Errorf("invalid UPDATE tuple tag %q (expected K, O, or N)", string(tag))
		}

		next, parsedOld, err := rt.parseTuple(rem, relID)
		if err != nil {
			return nil, walRecord{}, err
		}
		rem = next
		// Prefer full old tuple (O) when available, otherwise keep key tuple (K).
		if tag == 'O' || oldRow == nil {
			oldRow = parsedOld
		}
	}

	next, newRow, err := rt.parseTuple(rem, relID)
	if err != nil {
		return nil, walRecord{}, err
	}

	rel := rt.lookupRelation(relID)
	record := walRecord{
		LSN:          lsn,
		TableSchema:  rel.Schema,
		TableName:    rel.Table,
		Operation:    "Update",
		OID:          relID,
		NewTuple:     newRow,
		OldTuple:     oldRow,
		TxCommitTime: rt.state.txn.CommitTime,
		TxXID:        rt.state.txn.XID,
	}
	if err := record.prepareMessage(rt.cfg.topicPrefix); err != nil {
		return nil, walRecord{}, err
	}
	return next, record, nil
}

func (rt *runtime) parseDelete(buf []byte, lsn uint64) ([]byte, walRecord, error) {
	if len(buf) < 5 {
		return nil, walRecord{}, errors.New("short DELETE")
	}
	relID := binary.BigEndian.Uint32(buf[0:4])
	tag := buf[4]
	if tag != 'K' && tag != 'O' {
		return nil, walRecord{}, errors.New("invalid DELETE tuple tag")
	}
	rem, row, err := rt.parseTuple(buf[5:], relID)
	if err != nil {
		return nil, walRecord{}, err
	}
	rel := rt.lookupRelation(relID)
	partialOldTuple := rel.ReplicaIdentity != 'f'
	record := walRecord{
		LSN:             lsn,
		TableSchema:     rel.Schema,
		TableName:       rel.Table,
		Operation:       "Delete",
		OID:             relID,
		NewTuple:        nil,
		OldTuple:        row,
		PartialOldTuple: partialOldTuple,
		TxCommitTime:    rt.state.txn.CommitTime,
		TxXID:           rt.state.txn.XID,
	}
	if err := record.prepareMessage(rt.cfg.topicPrefix); err != nil {
		return nil, walRecord{}, err
	}
	return rem, record, nil
}

func (rt *runtime) parseTruncate(buf []byte, lsn uint64) ([]byte, []walRecord, error) {
	if len(buf) < 5 {
		return nil, nil, errors.New("short TRUNCATE")
	}
	count := int(binary.BigEndian.Uint32(buf[0:4]))
	rem := buf[5:] // skip options byte
	if len(rem) < count*4 {
		return nil, nil, errors.New("short TRUNCATE rel list")
	}
	records := make([]walRecord, 0, count)
	for i := 0; i < count; i++ {
		relID := binary.BigEndian.Uint32(rem[i*4 : i*4+4])
		rel := rt.lookupRelation(relID)
		record := walRecord{
			LSN:          lsn,
			TableSchema:  rel.Schema,
			TableName:    rel.Table,
			Operation:    "Truncate",
			OID:          relID,
			TxCommitTime: rt.state.txn.CommitTime,
			TxXID:        rt.state.txn.XID,
		}
		if err := record.prepareMessage(rt.cfg.topicPrefix); err != nil {
			return nil, nil, err
		}
		records = append(records, record)
	}
	return rem[count*4:], records, nil
}

func (rt *runtime) parseTuple(buf []byte, relID uint32) ([]byte, *rowData, error) {
	if len(buf) < 2 {
		return nil, nil, errors.New("short tuple")
	}
	count := int(binary.BigEndian.Uint16(buf[0:2]))
	rem := buf[2:]
	rel := rt.lookupRelation(relID)

	cols := make([]column, 0, count)
	for i := 0; i < count; i++ {
		if len(rem) < 1 {
			return nil, nil, errors.New("short tuple column tag")
		}
		tag := rem[0]
		rem = rem[1:]

		name := "col" + strconv.Itoa(i+1)
		typeID := uint32(0)
		if i < len(rel.Columns) {
			name = rel.Columns[i].Name
			typeID = rel.Columns[i].TypeOID
		}

		col := column{Name: name, TypeID: typeID}
		switch tag {
		case 'n':
			col.IsNull = true
		case 'u':
			col.Value = nil
			col.IsNull = false
		case 't':
			if len(rem) < 4 {
				return nil, nil, errors.New("short tuple text length")
			}
			ln := int(binary.BigEndian.Uint32(rem[0:4]))
			rem = rem[4:]
			if len(rem) < ln {
				return nil, nil, errors.New("short tuple text bytes")
			}
			col.Value = string(rem[:ln])
			col.IsNull = false
			rem = rem[ln:]
		default:
			return nil, nil, errors.New("unsupported tuple tag")
		}
		cols = append(cols, col)
	}

	return rem, &rowData{Columns: cols}, nil
}

func (rt *runtime) lookupRelation(relID uint32) relationMeta {
	if rel, ok := rt.state.relations[relID]; ok {
		return rel
	}
	return relationMeta{ID: relID, Schema: "public", Table: strconv.FormatUint(uint64(relID), 10), ReplicaIdentity: 'd'}
}

// publisher drains walRecords from recordQueue, batches them by topic, and
// writes each topic's messages concurrently via WriteMessages.  Concurrency
// eliminates the per-topic serialisation penalty: with two active topics
// (upi_transactions + users) the writes to each Kafka partition run in
// parallel, halving effective broker round-trip time.
//
// maxBatch is deliberately large (5000) to amortise the broker round-trip
// over more records, giving the publisher enough headroom to keep pace with
// higher CDC event rates.
func (rt *runtime) publisher(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case rec := <-rt.recordQueue:
			rt.enqueuePendingLSN(rec.LSN)
			rt.dispatchTopicRecord(ctx, rec)
		}
	}
}

func (rt *runtime) dispatchTopicRecord(ctx context.Context, rec walRecord) error {
	q := rt.getTopicQueue(ctx, rec.Topic)
	select {
	case q <- rec:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (rt *runtime) getTopicQueue(ctx context.Context, topic string) chan walRecord {
	rt.topicQueueMu.Lock()
	q, ok := rt.topicQueues[topic]
	if !ok {
		q = make(chan walRecord, rt.cfg.batchSize)
		rt.topicQueues[topic] = q
		go rt.topicWorker(ctx, topic, q)
	}
	rt.topicQueueMu.Unlock()
	return q
}

func (rt *runtime) topicWorker(ctx context.Context, topic string, q chan walRecord) {
	const maxBatch = 500
	records := make([]walRecord, 0, maxBatch)

	for {
		records = records[:0]

		select {
		case <-ctx.Done():
			return
		case rec := <-q:
			records = append(records, rec)
		}

	drain:
		for len(records) < maxBatch {
			select {
			case rec := <-q:
				records = append(records, rec)
			default:
				break drain
			}
		}

		msgs := make([]kafka.Message, 0, len(records))
		for _, rec := range records {
			msgs = append(msgs, kafka.Message{Topic: topic, Key: rec.Key, Value: rec.Payload})
		}

		for {
			var lastErr error
			backoff := 10 * time.Millisecond
			for i := 0; i < 6; i++ {
				writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				lastErr = rt.writer.WriteMessages(writeCtx, msgs...)
				cancel()
				if lastErr == nil {
					break
				}
				if ctx.Err() != nil {
					lastErr = ctx.Err()
					break
				}
				time.Sleep(backoff)
				if backoff < 500*time.Millisecond {
					backoff *= 2
				}
			}

			if lastErr != nil {
				for _, rec := range records {
					topicName := rt.cfg.topicPrefix + "." + rec.TableSchema + "." + rec.TableName
					rt.stats.failed.Add(1)
					rt.m.publishFailures.WithLabelValues(strings.ToLower(rec.Operation), topicName).Inc()
				}
				rt.logger.Printf("publish %s batch failed for topic=%s: %v; retrying in 1s", strings.ToLower(records[0].Operation), topic, lastErr)

				select {
				case <-ctx.Done():
					return
				case <-time.After(1 * time.Second):
				}
				continue
			}

			for _, rec := range records {
			topicName := rt.cfg.topicPrefix + "." + rec.TableSchema + "." + rec.TableName
			rt.stats.published.Add(1)
			rt.m.publishedRecords.WithLabelValues(strings.ToLower(rec.Operation), topicName).Inc()
			rt.markPublishedLSN(rec.LSN)
		}
	}
}

func (rt *runtime) enqueuePendingLSN(lsn uint64) {
	rt.pendingMu.Lock()
	rt.pendingLSNCounts[lsn]++
	rt.pendingMu.Unlock()
}

func (rt *runtime) markPublishedLSN(lsn uint64) {
	rt.pendingMu.Lock()
	if cnt, ok := rt.pendingLSNCounts[lsn]; ok {
		if cnt <= 1 {
			delete(rt.pendingLSNCounts, lsn)
		} else {
			rt.pendingLSNCounts[lsn] = cnt - 1
		}
	}

	for pending := range rt.pendingLSNCounts {
		if pending <= lsn {
			rt.pendingMu.Unlock()
			return
		}
	}
	rt.pendingMu.Unlock()

	for {
		current := rt.confirmedLSN.Load()
		if lsn <= current {
			return
		}
		if rt.confirmedLSN.CompareAndSwap(current, lsn) {
			return
		}
	}
}

func (rt *runtime) publishRecord(ctx context.Context, record walRecord) error {
	select {
	case rt.recordQueue <- record:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (rt *runtime) logStats(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	var lastPublished uint64
	var lastFailed uint64

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			published := rt.stats.published.Load()
			failed := rt.stats.failed.Load()
			rt.logger.Printf(
				"stats: ready=%t published_total=%d published_delta=%d failed_total=%d failed_delta=%d",
				rt.ready.Load(),
				published,
				published-lastPublished,
				failed,
				failed-lastFailed,
			)
			lastPublished = published
			lastFailed = failed
		}
	}
}

func slotLostError(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToUpper(err.Error())
	return strings.Contains(s, "SQLSTATE 55000") || strings.Contains(s, "CAN NO LONGER GET CHANGES FROM REPLICATION SLOT")
}

func (rt *runtime) resetReplicationSlot(ctx context.Context) error {
	adminConnStr := "host=" + rt.cfg.pgHost +
		" port=" + rt.cfg.pgPort +
		" user=" + rt.cfg.pgUser +
		" password=" + rt.cfg.pgPassword +
		" dbname=" + rt.cfg.pgDatabase

	adminConn, err := pgconn.Connect(ctx, adminConnStr)
	if err != nil {
		return redactPassword(err, rt.cfg.pgPassword)
	}
	defer adminConn.Close(context.Background())

	// Step 1: Terminate any backends holding the slot active.
	// This is necessary because PostgreSQL won't drop an active slot even after
	// the client disconnects until the backend process is terminated.
	terminateSQL := `
		SELECT pg_terminate_backend(active_pid)
		FROM pg_replication_slots
		WHERE slot_name = $1 AND active_pid IS NOT NULL
	`
	rr := adminConn.ExecParams(ctx, terminateSQL, [][]byte{[]byte(rt.cfg.pgSlotName)}, nil, nil, nil)
	if _, err := rr.Close(); err != nil {
		// Log but don't fail; the terminate may not have done anything if slot doesn't exist yet
		rt.logger.Printf("terminate backends for slot %s: %v", rt.cfg.pgSlotName, err)
	}

	// Small delay to allow PostgreSQL to clean up the terminated backend
	select {
	case <-time.After(500 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}

	// Step 2: Drop the slot (regardless of active state, since we just terminated all backends)
	dropSQL := "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)"
	rr = adminConn.ExecParams(ctx, dropSQL, [][]byte{[]byte(rt.cfg.pgSlotName)}, nil, nil, nil)
	if _, err := rr.Close(); err != nil {
		// If the slot doesn't exist, that's fine; we're going to create it anyway
		rt.logger.Printf("drop slot %s (may not exist): %v", rt.cfg.pgSlotName, err)
	}

	// Step 3: Create a fresh slot
	createSQL := "SELECT pg_create_logical_replication_slot($1, 'pgoutput', false, false)"
	rr = adminConn.ExecParams(ctx, createSQL, [][]byte{[]byte(rt.cfg.pgSlotName)}, nil, nil, nil)
	if _, err := rr.Close(); err != nil {
		return err
	}

	rt.m.slotResets.Inc()
	rt.logger.Printf("replication slot %s reset complete", rt.cfg.pgSlotName)
	return nil
}

func parseCString(buf []byte) (string, []byte, bool) {
	idx := bytes.IndexByte(buf, 0)
	if idx < 0 {
		return "", nil, false
	}
	return string(buf[:idx]), buf[idx+1:], true
}

// redactPassword replaces a literal password inside error messages to prevent
// credential leakage when pgconn embeds the full DSN in error text.
func redactPassword(err error, password string) error {
	if err == nil || password == "" {
		return err
	}
	return errors.New(strings.ReplaceAll(err.Error(), password, "[REDACTED]"))
}
