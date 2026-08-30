package main

import (
	"context"
	"database/sql"
	"flag"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/lewisdalwin/gatekeeper/internal/data"
	_ "github.com/lib/pq"
)

type config struct {
	port               int
	env                string
	reportDelay        time.Duration // artificial delay injected in the worker, used to make async-vs-sync measurable
	workerPollInterval time.Duration // how often the worker checks the jobs table for new work
	db                 struct {
		dsn          string
		maxOpenConns int
		maxIdleConns int
		maxIdleTime  time.Duration
	}
}

type application struct {
	config       config
	logger       *slog.Logger
	models       data.Models
	wg           sync.WaitGroup     // wg tracks the background worker goroutine so shutdown can wait for it to actually stop before the process exits
	workerCancel context.CancelFunc // workerCancel is the cancel function for the worker's own context.
}

func main() {
	var cfg config

	flag.IntVar(&cfg.port, "port", 4000, "API server port")
	flag.StringVar(&cfg.env, "env", "development", "Environment (development|staging|production)")
	flag.DurationVar(&cfg.reportDelay, "report-delay", 0, "Artificial report-generation delay inside the worker")
	flag.DurationVar(&cfg.workerPollInterval, "worker-poll-interval", 250*time.Millisecond, "Worker queue-check interval")

	flag.StringVar(&cfg.db.dsn, "db-dsn", "postgresql://alex:alex@localhost/asyncdb?sslmode=disable", "PostgreSQL DSN")

	flag.IntVar(&cfg.db.maxOpenConns, "db-max-open-conns", 25, "PostgreSQL max open connections")
	flag.IntVar(&cfg.db.maxIdleConns, "db-max-idle-conns", 25, "PostgreSQL max idle connections")
	flag.DurationVar(&cfg.db.maxIdleTime, "db-max-idle-time", 15*time.Minute, "PostgreSQL max connection idle time")

	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	db, err := openDB(cfg)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	defer db.Close()

	logger.Info("database connection pool established")

	app := &application{
		config: cfg,
		logger: logger,
		models: data.NewModels(db),
	}

	// The worker gets its OWN context, separate from any per-request
	workerCtx, cancelWorker := context.WithCancel(context.Background())
	app.workerCancel = cancelWorker
	defer cancelWorker() // safety net: guarantees workerCtx is cancelled even if serve() returns via an error path that skips the normal shutdown sequence
	app.startReportWorker(workerCtx)

	err = app.serve() // blocks until the server shuts dow
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
}

// openDB opens the connection pool and verifies connectivity
func openDB(cfg config) (*sql.DB, error) {
	db, err := sql.Open("postgres", cfg.db.dsn)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(cfg.db.maxOpenConns)
	db.SetMaxIdleConns(cfg.db.maxIdleConns)
	db.SetConnMaxIdleTime(cfg.db.maxIdleTime)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = db.PingContext(ctx)
	if err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}
