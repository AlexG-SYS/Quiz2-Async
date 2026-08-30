package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// serve starts the HTTP server and listens for incoming requests.
// It also handles graceful shutdown when receiving termination signals.
func (app *application) serve() error {
	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", app.config.port),
		Handler:      app.routes(),
		IdleTimeout:  time.Minute,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	shutdownError := make(chan error)

	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		s := <-quit

		app.logger.Info("caught signal", "signal", s.String())

		// Create a context with a timeout to ensure the server shuts down gracefully within 30 seconds.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		err := srv.Shutdown(ctx)
		if err != nil {
			shutdownError <- err
			return
		}

		app.logger.Info("completing background tasks", "addr", srv.Addr)

		// Order matters here: cancel the worker's context FIRST so its
		// polling loop can observe ctx.Done() and exit on its own, THEN
		// wait for it to actually finish. If wg.Wait() were called without
		// ever cancelling the worker, it would block forever — the
		// worker's for/select loop has no other way to know it should
		// stop.
		if app.workerCancel != nil {
			app.workerCancel()
		}
		app.wg.Wait() // blocks until the worker goroutine's deferred wg.Done() runs
		shutdownError <- nil
	}()

	app.logger.Info("starting server", "addr", srv.Addr, "env", app.config.env)

	// ListenAndServe blocks until the server stops.
	// If the server is stopped due to a shutdown signal,
	// it will return http.ErrServerClosed,
	// which we can ignore. Any other error should be returned.
	err := srv.ListenAndServe()
	if !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	// Wait for the shutdown goroutine to finish and return any error it encountered.
	// (HTTP shutdown -> worker cancel -> wg.Wait)
	err = <-shutdownError
	if err != nil {
		return err
	}

	app.logger.Info("stopped server", "addr", srv.Addr)

	return nil
}
