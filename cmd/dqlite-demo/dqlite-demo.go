package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"text/tabwriter"

	"github.com/canonical/go-dqlite/app"
	"github.com/canonical/go-dqlite/client"
	"github.com/juju/pubsub"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"golang.org/x/sys/unix"
)

func main() {
	var api string
	var db string
	var join *[]string
	var dir string
	var verbose bool

	cmd := &cobra.Command{
		Use:   "dqlite-demo",
		Short: "Demo application using dqlite",
		Long: `This demo shows how to integrate a Go application with dqlite.

Complete documentation is available at https://github.com/canonical/go-dqlite`,
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := filepath.Join(dir, db)
			if err := os.MkdirAll(dir, 0755); err != nil {
				return errors.Wrapf(err, "can't create %s", dir)
			}
			logFunc := func(l client.LogLevel, format string, a ...interface{}) {
				if !verbose {
					return
				}
				log.Printf(fmt.Sprintf("%s: %s: %s\n", api, l.String(), format), a...)
			}

			events := pubsub.NewSimpleHub(nil)
			events.SubscribeMatch(pubsub.MatchAll, func(topic string, data interface{}) {
				fmt.Println("!!", topic, data)
			})

			app, err := app.New(dir, app.WithAddress(db), app.WithCluster(*join), app.WithLogFunc(logFunc))
			if err != nil {
				return err
			}

			if err := app.Ready(context.Background()); err != nil {
				return err
			}

			db, err := app.Open(context.Background(), "demo")
			if err != nil {
				return err
			}

			if _, err := db.Exec(schema); err != nil {
				return err
			}

			http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				key := strings.TrimLeft(r.URL.Path, "/")
				result := ""
				switch r.Method {
				case "GET":
					row := db.QueryRow(query, key)
					var id int
					var value string
					if err := row.Scan(&id, &value); err != nil {
						result = fmt.Sprintf("Error: %s", err.Error())
						break
					}
					result = fmt.Sprintf("%d: %s", id, value)

				case "PUT":
					result = "done"
					value, _ := ioutil.ReadAll(r.Body)
					if _, err := db.Exec(update, key, value); err != nil {
						result = fmt.Sprintf("Error: %s", err.Error())
					}

				case "POST": // Just for a test
					rows, err := db.Query(wal)
					if err != nil {
						result = fmt.Sprintf("Error: %s", err.Error())
						break
					}
					defer rows.Close()

					var docs []struct {
						t  string
						id int
						m  string
					}
					dest := func(i int) []interface{} {
						docs = append(docs, struct {
							t  string
							id int
							m  string
						}{})
						return []interface{}{
							&docs[i].t,
							&docs[i].id,
							&docs[i].m,
						}
					}

					for i := 0; rows.Next(); i++ {
						if err := rows.Scan(dest(i)...); err != nil {
							result = fmt.Sprintf("Error: %s", err.Error())
							break
						}
					}

					if result == "" {
						buf := new(bytes.Buffer)
						tab := tabwriter.NewWriter(buf, 2, 4, 2, '\t', 0)
						for k, v := range docs {
							fmt.Fprintf(tab, "%d\t%s\t%d\t%s\n", k, v.t, v.id, v.m)
						}
						tab.Flush()
						result = buf.String()
					}

				default:
					result = fmt.Sprintf("Error: unsupported method %q", r.Method)

				}
				fmt.Fprintf(w, "%s\n", result)
			})

			listener, err := net.Listen("tcp", api)
			if err != nil {
				return err
			}

			go http.Serve(listener, nil)

			ch := make(chan os.Signal, 1)
			signal.Notify(ch, unix.SIGPWR)
			signal.Notify(ch, unix.SIGINT)
			signal.Notify(ch, unix.SIGQUIT)
			signal.Notify(ch, unix.SIGTERM)

			<-ch

			listener.Close()
			db.Close()

			app.Handover(context.Background())
			app.Close()

			return nil
		},
	}

	flags := cmd.Flags()
	flags.StringVarP(&api, "api", "a", "", "address used to expose the demo API")
	flags.StringVarP(&db, "db", "d", "", "address used for internal database replication")
	join = flags.StringSliceP("join", "j", nil, "database addresses of existing nodes")
	flags.StringVarP(&dir, "dir", "D", "/tmp/dqlite-demo", "data directory")
	flags.BoolVarP(&verbose, "verbose", "v", false, "verbose logging")

	cmd.MarkFlagRequired("api")
	cmd.MarkFlagRequired("db")

	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}

const (
	schema = `
CREATE TABLE IF NOT EXISTS model (
	id INTEGER PRIMARY KEY AUTOINCREMENT, 
	key TEXT, 
	value TEXT, 
	UNIQUE(key)
);
	
CREATE TABLE IF NOT EXISTS wal (
	type TEXT, 
	id INTEGER, 
	method TEXT, 
	created_at DATETIME
);
	
CREATE TRIGGER IF NOT EXISTS insert_model_trigger
AFTER INSERT ON model FOR EACH ROW
BEGIN
	INSERT INTO wal VALUES ("model", NEW.id, "insert", CURRENT_TIMESTAMP);
END;

CREATE TRIGGER IF NOT EXISTS update_model_trigger
AFTER UPDATE ON model FOR EACH ROW
BEGIN
	INSERT INTO wal VALUES ("model", NEW.id, "update", CURRENT_TIMESTAMP);
END;
`
	query  = "SELECT id, value FROM model WHERE key = ?"
	update = "INSERT OR REPLACE INTO model(key, value) VALUES(?, ?)"
	wal    = "SELECT type, id, method FROM wal"
)
