package dqlite

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"time"

	"github.com/CanonicalLtd/go-dqlite/internal/bindings"
	"github.com/CanonicalLtd/go-dqlite/internal/client"
	"github.com/Rican7/retry/backoff"
	"github.com/Rican7/retry/strategy"
	"github.com/pkg/errors"
)

// ServerInfo holds information about a single server.
type ServerInfo = client.ServerInfo

// Server implements the dqlite network protocol.
type Server struct {
	log      LogFunc          // Logger
	server   *bindings.Server // Low-level C implementation
	listener net.Listener     // Queue of new connections
	runCh    chan error       // Receives the low-level C server return code
	acceptCh chan error       // Receives connection handling errors
	id       uint64
	address  string
}

// ServerOption can be used to tweak server parameters.
type ServerOption func(*serverOptions)

// WithServerLogFunc sets a custom log function for the server.
func WithServerLogFunc(log LogFunc) ServerOption {
	return func(options *serverOptions) {
		options.Log = log
	}
}

// WithServerDialFunc sets a custom dial function for the server.
func WithServerDialFunc(dial DialFunc) ServerOption {
	return func(options *serverOptions) {
		options.DialFunc = dial
	}
}

// NewServer creates a new Server instance.
func NewServer(info ServerInfo, dir string, options ...ServerOption) (*Server, error) {
	o := defaultServerOptions()

	for _, option := range options {
		option(o)
	}

	server, err := bindings.NewServer(uint(info.ID), info.Address, dir)
	if err != nil {
		return nil, err
	}
	if o.DialFunc != nil {
		server.SetDialFunc(bindings.DialFunc(o.DialFunc))
	}

	s := &Server{
		log:      o.Log,
		server:   server,
		runCh:    make(chan error),
		acceptCh: make(chan error, 1),
		id:       info.ID,
		address:  info.Address,
	}

	return s, nil
}

// Bootstrap the server.
func (s *Server) Bootstrap(servers []ServerInfo) error {
	return s.server.Bootstrap(servers)
}

// Start serving requests.
func (s *Server) Start(listener net.Listener) error {
	go s.run()

	s.listener = listener

	if !s.server.Ready() {
		return fmt.Errorf("server failed to start")
	}

	go s.acceptLoop()

	return nil
}

// Join a cluster.
func (s *Server) Join(ctx context.Context, info ServerInfo) error {
	store := NewInmemServerStore()
	config := client.Config{
		Dial:           client.TCPDial,
		AttemptTimeout: 100 * time.Millisecond,
		RetryStrategies: []strategy.Strategy{
			strategy.Backoff(backoff.BinaryExponential(time.Millisecond))},
	}
	store.Set(ctx, []ServerInfo{info})
	connector := client.NewConnector(0, store, config, defaultLogFunc())
	c, err := connector.Connect(ctx)
	if err != nil {
		return err
	}
	defer c.Close()

	request := client.Message{}
	request.Init(4096)
	response := client.Message{}
	response.Init(4096)

	client.EncodeJoin(&request, s.id, s.address)

	if err := c.Call(ctx, &request, &response); err != nil {
		return err
	}

	return nil
}

// Hold configuration options for a dqlite server.
type serverOptions struct {
	Log      LogFunc
	DialFunc DialFunc
}

// Run the server.
func (s *Server) run() {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	s.runCh <- s.server.Run()
}

func (s *Server) acceptLoop() {
	s.log(LogDebug, "accepting connections")

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			s.acceptCh <- nil
			return
		}

		err = s.server.Handle(conn)
		if err != nil {
			if err == bindings.ErrServerStopped {
				// Ignore failures due to the server being
				// stopped.
				err = nil
			}
			s.acceptCh <- err
			return
		}
	}
}

// Dump the files of a database to disk.
// func (s *Server) Dump(name string, dir string) error {
// 	// Dump the database file.
// 	bytes, err := s.registry.vfs.ReadFile(name)
// 	if err != nil {
// 		return errors.Wrap(err, "failed to get database file content")
// 	}

// 	path := filepath.Join(dir, name)
// 	if err := ioutil.WriteFile(path, bytes, 0600); err != nil {
// 		return errors.Wrap(err, "failed to write database file")
// 	}

// 	// Dump the WAL file.
// 	bytes, err = s.registry.vfs.ReadFile(name + "-wal")
// 	if err != nil {
// 		return errors.Wrap(err, "failed to get WAL file content")
// 	}

// 	path = filepath.Join(dir, name+"-wal")
// 	if err := ioutil.WriteFile(path, bytes, 0600); err != nil {
// 		return errors.Wrap(err, "failed to write WAL file")
// 	}

// 	return nil
// }

// Close the server, releasing all resources it created.
func (s *Server) Close() error {
	if s.listener == nil {
		goto out
	}

	// Close the listener, which will make the listener.Accept() call in
	// acceptLoop() return an error.
	if err := s.listener.Close(); err != nil {
		return err
	}

	// Wait for the acceptLoop goroutine to exit.
	select {
	case err := <-s.acceptCh:
		if err != nil {
			return errors.Wrap(err, "accept goroutine failed")
		}
	case <-time.After(time.Second):
		return fmt.Errorf("accept goroutine did not stop within a second")
	}

	// Send a stop signal to the dqlite event loop.
	if err := s.server.Stop(); err != nil {
		return errors.Wrap(err, "server failed to stop")
	}

	// Wait for the run goroutine to exit.
	select {
	case err := <-s.runCh:
		if err != nil {
			return errors.Wrap(err, "accept goroutine failed")
		}
	case <-time.After(time.Second):
		return fmt.Errorf("server did not stop within a second")
	}

out:
	s.server.Close()

	return nil
}

// Create a serverOptions object with sane defaults.
func defaultServerOptions() *serverOptions {
	return &serverOptions{
		Log: defaultLogFunc(),
	}
}
