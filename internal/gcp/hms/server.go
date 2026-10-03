package hms

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/apache/thrift/lib/go/thrift"

	hmsstore "jaiscloud/internal/gcp/store/hms"
)

// Server is the Hive Metastore Thrift serving plane: a raw TCP listener
// speaking TBinaryProtocol over a non-framed TSocket (no TFramedTransport —
// that is TCLIService/HiveServer2, a different protocol). It parallels the
// gRPC listener in internal/gcp/grpc: a separate port, its own accept loop, and
// the same store/lock discipline shared with the control plane. The catalog is
// single-global (F6) — databases/tables are keyed by name only.
type Server struct {
	addr    string
	store   hmsstore.Store
	sock    *thrift.TServerSocket
	mu      sync.Mutex
	wg      sync.WaitGroup
	methods map[string]methodHandler
}

// NewServer returns a Thrift serving-plane server bound to addr (e.g.
// ":9083"), backed by the given store.
func NewServer(addr string, store hmsstore.Store) *Server {
	s := &Server{addr: addr, store: store}
	s.methods = s.methodTable()
	return s
}

// methodTable builds the method dispatch map. Names are the exact
// hive_metastore.thrift method names (ThriftHiveMetastore service).
func (s *Server) methodTable() map[string]methodHandler {
	return map[string]methodHandler{
		// Databases.
		"create_database":   s.createDatabase,
		"get_database":      s.getDatabase,
		"drop_database":     s.dropDatabase,
		"get_databases":     s.listDatabases,
		"get_all_databases": s.listDatabases,
		"alter_database":    s.alterDatabase,

		// Tables.
		"create_table":                          s.createTable,
		"create_table_with_environment_context": s.createTable,
		"get_table":                             s.getTable,
		"get_table_req":                         s.getTableReq, // Hive 2.3+ request-struct form
		"get_table_by_name":                     s.getTable,    // alias: same (dbname, tbl_name) -> Table shape
		"get_all_tables":                        s.listTables,
		"get_tables":                            s.listTables,
		"get_table_objects_by_name":             s.getTableObjectsByName,
		"get_table_objects_by_name_req":         s.getTableObjectsByNameReq, // Hive 2.3+ request-struct form
		"alter_table":                           s.alterTable,
		"alter_table_with_environment_context":  s.alterTable,
		"alter_table_with_cascade":              s.alterTable, // cascade ignored: partitions keep their own SD (MP9)
		"get_table_meta":                        s.getTableMeta,
		"drop_table":                            s.dropTable,
		"drop_table_with_environment_context":   s.dropTable,

		// Functions (stubbed).
		"create_function":   s.createFunction,
		"drop_function":     s.dropFunction,
		"get_functions":     s.getFunctions,
		"get_function":      s.getFunction,
		"get_all_functions": s.getAllFunctions,

		// Concurrency (Iceberg commit locking).
		"lock":       s.lock,
		"check_lock": s.checkLock,
		"unlock":     s.unlock,

		// Identity: set_ugi is the caller-identity announcement Hive clients
		// issue once per new binary-NOSASL connection. Without it, strict
		// clients (e.g. hms-client-go) refuse to connect.
		"set_ugi": s.setUgi,

		// Misc (valid empty stubs).
		"get_current_notificationEventId": s.getCurrentNotificationEventID,
		"get_next_notification":           s.getNextNotification,

		// Partitions — full CRUD over the jc_hms_partitions store (generic Hive
		// path; Iceberg keeps its own partition metadata and does not call these).
		"add_partition":                                     s.addPartition,
		"add_partition_with_environment_context":            s.addPartition,
		"add_partitions":                                    s.addPartitions,
		"add_partitions_req":                                s.addPartitionsReq,
		"add_partitions_pspec":                              s.addPartitionsPspec,
		"append_partition":                                  s.appendPartition,
		"append_partition_with_environment_context":         s.appendPartition,
		"append_partition_by_name":                          s.appendPartitionByName,
		"append_partition_by_name_with_environment_context": s.appendPartitionByName,
		"get_partition":                                     s.getPartition,
		"get_partition_with_auth":                           s.getPartition,
		"get_partition_by_name":                             s.getPartitionByName,
		"get_partitions":                                    s.getPartitions,
		"get_partitions_with_auth":                          s.getPartitions,
		"get_partitions_ps":                                 s.getPartitionsPs,
		"get_partitions_ps_with_auth":                       s.getPartitionsPs,
		"get_partitions_by_filter":                          s.getPartitionsByFilter,
		"get_partitions_by_names":                           s.getPartitionsByNames,
		"get_part_specs_by_filter":                          s.getPartSpecsByFilter,
		"get_partitions_pspec":                              s.getPartitionsPspec,
		"get_num_partitions_by_filter":                      s.getNumPartitionsByFilter,
		"get_partition_names":                               s.getPartitionNames,
		"get_partition_names_ps":                            s.getPartitionNamesPs,
		"get_partition_names_ps_with_auth":                  s.getPartitionNamesPs,
		"partition_name_to_vals":                            s.partitionNameToValsHandler,
		"partition_name_to_spec":                            s.partitionNameToSpecHandler,
		"alter_partition":                                   s.alterPartition,
		"alter_partition_with_environment_context":          s.alterPartition,
		"alter_partitions":                                  s.alterPartitions,
		"alter_partitions_with_environment_context":         s.alterPartitions,
		"rename_partition":                                  s.renamePartition,
		"drop_partition":                                    s.dropPartition,
		"drop_partition_with_environment_context":           s.dropPartition,
		"drop_partition_by_name":                            s.dropPartitionByName,
		"drop_partition_by_name_with_environment_context":   s.dropPartitionByName,
		"partition_name_has_valid_characters":               s.stubPartitionBoolTrue,

		// Deferred partition methods — honest unsupported failure, never a silent
		// ACK (they are niche and have no AWS Glue analogue; recorded in the plan).
		"exchange_partition":        s.unsupportedMethod,
		"exchange_partitions":       s.unsupportedMethod,
		"drop_partitions_req":       s.unsupportedMethod,
		"get_partition_values":      s.unsupportedMethod,
		"get_partitions_by_expr":    s.unsupportedMethod,
		"markPartitionForEvent":     s.unsupportedMethod,
		"isPartitionMarkedForEvent": s.unsupportedMethod,

		// Out-of-scope ACID/txn methods — honest failure, never a silent ACK.
		"heartbeat":           s.heartbeat,
		"heartbeat_txn_range": s.unsupportedMethod,
		"get_open_txns":       s.unsupportedMethod,
		"get_open_txns_info":  s.unsupportedMethod,
		"open_txns":           s.unsupportedMethod,
		"abort_txn":           s.unsupportedMethod,
		"abort_txns":          s.unsupportedMethod,
		"commit_txn":          s.unsupportedMethod,
	}
}

// Serve listens on the configured address and blocks serving until Stop or the
// process exits. Each connection is handled in its own goroutine.
func (s *Server) Serve() error {
	sock, err := thrift.NewTServerSocket(s.addr)
	if err != nil {
		return fmt.Errorf("hms thrift listen on %s: %w", s.addr, err)
	}
	if err := sock.Listen(); err != nil {
		return fmt.Errorf("hms thrift listen on %s: %w", s.addr, err)
	}
	s.mu.Lock()
	s.sock = sock
	s.mu.Unlock()

	for {
		transport, err := sock.Accept()
		if err != nil {
			// Interrupted / closed — normal shutdown.
			return nil
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer transport.Close()
			s.handleConn(transport)
		}()
	}
}

// Stop gracefully stops the listener and waits for in-flight connections.
func (s *Server) Stop() {
	s.mu.Lock()
	sock := s.sock
	s.sock = nil
	s.mu.Unlock()
	if sock != nil {
		_ = sock.Interrupt()
	}
	s.wg.Wait()
}

// handleConn serves one connection: read a message, dispatch, write the reply,
// repeat until EOF or a transport error. Requests are processed sequentially on
// a connection, matching the Hive metastore client's request/response model.
func (s *Server) handleConn(transport thrift.TTransport) {
	proto := thrift.NewTBinaryProtocolTransport(transport)
	ctx := context.Background()
	for {
		name, _, seqid, err := proto.ReadMessageBegin(ctx)
		if err != nil {
			return
		}
		args, err := ReadStruct(ctx, proto)
		if err != nil {
			return
		}
		result, herr := s.dispatch(ctx, name, args)
		if werr := s.writeReply(ctx, proto, name, seqid, result, herr); werr != nil {
			return
		}
	}
}

// dispatch routes a method name to its handler, falling back to an
// UNKNOWN_METHOD TApplicationException so the codec never hard-fails on an
// unknown call.
func (s *Server) dispatch(ctx context.Context, name string, args *Struct) (*Struct, error) {
	if h, ok := s.methods[name]; ok {
		return h(ctx, args)
	}
	return nil, &AppError{Type: thrift.UNKNOWN_METHOD, Message: "unknown method " + name}
}

// writeReply writes the reply message: a REPLY result struct, a REPLY carrying
// a declared exception field, or an EXCEPTION TApplicationException.
func (s *Server) writeReply(ctx context.Context, proto thrift.TProtocol, name string, seqid int32, result *Struct, herr error) error {
	switch err := herr.(type) {
	case nil:
		if err := proto.WriteMessageBegin(ctx, name, thrift.REPLY, seqid); err != nil {
			return err
		}
		if err := WriteStruct(ctx, proto, result); err != nil {
			return err
		}
		return proto.WriteMessageEnd(ctx)
	case *DeclaredError:
		if err := proto.WriteMessageBegin(ctx, name, thrift.REPLY, seqid); err != nil {
			return err
		}
		rs := &Struct{Fields: []Field{ExceptionField(err)}}
		if err := WriteStruct(ctx, proto, rs); err != nil {
			return err
		}
		return proto.WriteMessageEnd(ctx)
	case *AppError:
		if err := proto.WriteMessageBegin(ctx, name, thrift.EXCEPTION, seqid); err != nil {
			return err
		}
		if err := WriteAppException(ctx, proto, err.Type, err.Message); err != nil {
			return err
		}
		return proto.WriteMessageEnd(ctx)
	default:
		slog.Warn("hms: handler returned non-thrift error", "method", name, "err", err)
		if werr := proto.WriteMessageBegin(ctx, name, thrift.EXCEPTION, seqid); werr != nil {
			return werr
		}
		if werr := WriteAppException(ctx, proto, thrift.INTERNAL_ERROR, err.Error()); werr != nil {
			return werr
		}
		return proto.WriteMessageEnd(ctx)
	}
}

// Reset clears transport-level state (there is none beyond the shared store,
// which the admin handler resets separately). Present for symmetry with the
// gRPC server's Reset.
func (s *Server) Reset(context.Context) {}

// Addr returns the listener's bound address, or the configured address before
// Serve has started.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sock != nil {
		return s.sock.Addr()
	}
	return nil
}
