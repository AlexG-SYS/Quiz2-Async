package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

// startReportWorker launches exactly one long-lived background goroutine
// for the life of the process. It is called once from main(), never per
// request — a per-request worker would defeat the purpose (every request
// would spin up its own polling loop) and would have no well-defined
// lifetime to cancel on shutdown.
func (app *application) startReportWorker(ctx context.Context) {
	// wg.Add(1) happens BEFORE the goroutine starts, from the calling
	// goroutine. Doing it inside the new goroutine would race: server.go's
	// shutdown path could call wg.Wait() before the new goroutine gets a
	// chance to register itself, and Wait() would return immediately
	// without actually waiting for the worker.
	app.wg.Add(1)
	go func() {
		defer app.wg.Done() // guarantees the WaitGroup is decremented no matter how this goroutine exits
		ticker := time.NewTicker(app.config.workerPollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				// The context was canceled, which means the server is shutting down.
				// We can log this event and exit the goroutine gracefully.
				app.logger.Info("report worker stopped")
				return
			case <-ticker.C:
				// The ticker has ticked, so we should attempt to process the next report job.
				// Fires once per workerPollInterval. This is what bounds
				// the delay between a job being queued and the worker
				// noticing it: worst case, a job waits up to one interval
				// before being picked up.
				err := app.processNextReportJob(ctx)
				if err != nil && !errors.Is(err, sql.ErrNoRows) && !errors.Is(err, context.Canceled) {
					// Log any error that occurs during job processing, except for the expected
					// "no rows" error (which means there were no jobs to process) and context cancellation.
					// This ensures that we don't log unnecessary errors when the worker is shutting down.
					app.logger.Error("report worker failed", "error", err)
				}
			}
		}
	}()
}

// processNextReportJob attempts to claim and process the next available report job.
// It returns an error if there are no jobs to process or if any step in the job processing fails.
// run the simulated delay, generate the report, and record the
// outcome. Called once per tick from the loop above.
func (app *application) processNextReportJob(ctx context.Context) error {
	job, err := app.models.Jobs.ClaimNext(ctx)
	if err != nil {
		return err
	}
	app.logger.Info("report job started", "job_id", job.PublicID,
		"artificial_delay", app.config.reportDelay)

	// This artificial delay simulates "report generation is slow" for
	// measurement purposes. It must live HERE, inside the worker, not in
	// the POST handler — the whole point of the async design is that the
	// client's original request (createReportHandler) already returned
	// 202 before this line ever runs. If this sleep were in the POST
	// handler instead, the request would block for reportDelay and the
	// architecture would be synchronous
	if app.config.reportDelay > 0 {
		timer := time.NewTimer(app.config.reportDelay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			// Lets a shutdown in progress interrupt the artificial wait
			// immediately instead of blocking graceful shutdown for the
			// full delay duration.
			return ctx.Err()
		case <-timer.C:
		}
	}

	report, err := app.models.Reports.Generate(job.ConsumerID, job.Payload.From, job.Payload.To)
	// Report generation failing (e.g. the consumer was deleted after
	// the job was queued) is a legitimate, expected outcome, not a
	// worker crash: it is recorded as a normal failed job so the
	// client can see error_message via GET /v1/jobs/{id}.
	if err != nil {
		return app.models.Jobs.MarkFailed(ctx, job.ID, err.Error())
	}
	result, err := json.Marshal(report)
	if err != nil {
		return app.models.Jobs.MarkFailed(ctx, job.ID, err.Error())
	}
	if err := app.models.Jobs.MarkCompleted(ctx, job.ID, result); err != nil {
		return err
	}
	app.logger.Info("report job completed", "job_id", job.PublicID)
	return nil
}
