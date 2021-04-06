package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/jackc/pgx/v4"
	"math/rand"
	"os"
	"os/user"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	_ "github.com/lib/pq"
)

type LockableConnection struct {
	Conn *pgx.Conn
	Lock chan bool
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func checkoutConnection(connCount int, conns []*LockableConnection) *LockableConnection {
	for {
		candidate := rand.Intn(connCount)
		lock := conns[candidate].Lock
		select {
			case <-lock:
			default:
				continue
		}

		return conns[candidate]
	}
}

func main() {
	var queriesStr string
	flag.StringVar(&queriesStr, "queries", "select 1;", "Semicolon-separated list of queries to randomly select")
	var connCount int
	flag.IntVar(&connCount, "connCount", 10, "Number of connections to establish")
	var activeQueryCount int
	flag.IntVar(&activeQueryCount, "queryCount", 5, "Number of active queries to run at a time")

	var host string
	flag.StringVar(&host, "host", getEnv("PGHOST", "localhost"), "Host to connect to")
	defaultPort, err := strconv.Atoi(getEnv("PGPORT", "5432"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	var port int
	flag.IntVar(&port, "port", defaultPort, "Port to connect to")
	currentUser, err := user.Current()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	var user string
	flag.StringVar(&user, "user", getEnv("PGUSER", currentUser.Username), "User to connect with")
	var database string
	flag.StringVar(&database, "database", getEnv("PGDATABASE", currentUser.Username), "Database to connect to")

	flag.Parse()
	queries := strings.Split(queriesStr, ";")

	dbUrl := fmt.Sprintf("postgresql://%s@%s:%d/%s?application_name=loadGenerator", user, host, port, database)
	fmt.Printf("Establishing connections to %s\n", dbUrl)
	connections := make([]*LockableConnection, connCount)
	for i := 0; i < connCount; i++ {
		conn, err := pgx.Connect(context.Background(), dbUrl)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error connecting to db: %v\n", err)
			os.Exit(1)
		}
		lock := make(chan bool, 1)
		lock <- true
		//defer conn.Close(context.Background())
		connections[i] = &LockableConnection {
			Conn: conn,
			Lock: lock,
		}
	}
	numQueries := len(queries)
	queryCount := int32(0)
	errorCount := int32(0)
	for i := 0; i < activeQueryCount; i++ {
		go func() {
			for {
				conn := checkoutConnection(connCount, connections)
				_, err = conn.Conn.Exec(context.Background(), queries[rand.Intn(numQueries)])
				if err != nil {
					fmt.Fprintf(os.Stderr, "error executing query: %v\n", err)
					atomic.AddInt32(&errorCount, 1)
				}
				atomic.AddInt32(&queryCount, 1)
				conn.Lock <- true
			}
		}()
	}

	start := time.Now()
	for {
		time.Sleep(10 * time.Second)
		t := time.Now()
		elapsed := t.Sub(start) / time.Second
		fmt.Printf("[%d:%d] Executed %d queries (%d errors)\n", elapsed / 60, elapsed % 60, queryCount, errorCount)
	}

}
