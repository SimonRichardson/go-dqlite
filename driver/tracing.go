package driver

import (
	"context"
	"database/sql/driver"
	"sync"
	"time"

	"github.com/canonical/go-dqlite/client"
	"github.com/canonical/go-dqlite/logging"
	"github.com/canonical/go-dqlite/tracing"
)

type LogTracingDriver struct {
	driver    *Driver
	logTracer logTracer
}

func NewLogTracingDriver(driver *Driver, log logging.Func, logLevel client.LogLevel) driver.Driver {
	return &LogTracingDriver{
		driver: driver,
		logTracer: logTracer{
			log:      log,
			logLevel: logLevel,
		},
	}
}

func (d *LogTracingDriver) Open(uri string) (driver.Conn, error) {
	conn, err := d.driver.Open(uri)
	if err != nil {
		return nil, err
	}
	return &logTracingConn{
		Conn:      conn.(*Conn),
		logTracer: d.logTracer,
	}, nil
}

type logTracingConn struct {
	*Conn
	logTracer tracing.Tracer
}

func (c *logTracingConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return c.Conn.PrepareContext(tracing.WithTracer(ctx, c.logTracer), query)
}

func (c *logTracingConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.Conn.ExecContext(tracing.WithTracer(ctx, c.logTracer), query, args)
}

// QueryContext is an optional interface that may be implemented by a Conn.
func (c *logTracingConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.Conn.QueryContext(tracing.WithTracer(ctx, c.logTracer), query, args)
}

func (c *logTracingConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.Conn.BeginTx(tracing.WithTracer(ctx, c.logTracer), opts)
}

// logTracer is a tracing.Tracer that is used for backwards compatibility with
// the old tracing API to the logging API.
type logTracer struct {
	log      logging.Func
	logLevel client.LogLevel
}

// Start creates a span for a given query, that will be logged when the
// span is ended.
func (l logTracer) Start(ctx context.Context, name, query string) (context.Context, tracing.Span) {
	return ctx, &logTrace{
		log: func(msg string, args ...interface{}) {
			l.log(l.logLevel, msg, args...)
		},
		start: time.Now(),
		name:  name,
		query: query,
	}
}

type logTrace struct {
	log   func(msg string, args ...interface{})
	start time.Time
	name  string
	query string
	once  sync.Once
}

// End logs the end of the span. Multiple calls to End() will only log the
// span once.
func (l *logTrace) End() {
	l.once.Do(func() {
		l.log("%.3fs %s: %q", time.Since(l.start).Seconds(), l.name, l.query)
	})
}
