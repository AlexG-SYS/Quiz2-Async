package main

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/lewisdalwin/gatekeeper/internal/data"
	"github.com/lewisdalwin/gatekeeper/internal/validator"
)

// createReportHandler is the async entry point. Its entire job is to
// validate the request. it never runs the report generation itself, which is done by the worker. It returns
// a 202 Accepted response with a Location header pointing to the job status endpoint.
func (app *application) createReportHandler(w http.ResponseWriter, r *http.Request) {
	var input struct {
		ConsumerID string    `json:"consumer_id"`
		From       time.Time `json:"from"`
		To         time.Time `json:"to"`
	}
	if err := app.readJSON(w, r, &input); err != nil {
		app.badRequestResponse(w, r, err)
		return
	}

	v := validator.New()
	v.Check(input.ConsumerID != "", "consumer_id", "must be provided")
	v.Check(!input.From.IsZero(), "from", "must be provided")
	v.Check(!input.To.IsZero(), "to", "must be provided")
	v.Check(input.From.Before(input.To), "from", "must be earlier than to")
	if !v.Valid() {
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	// create a new job in the database for the worker to pick up. The worker will generate the report and update the job status when done.
	// ClaimNext (jobs.go) only picks up rows
	// with job_type = 'consumer_activity_report', so this string is the
	// contract between "what got queued" and "what the worker knows how to
	// process".
	job := &data.Job{
		ConsumerID: input.ConsumerID,
		JobType:    "consumer_activity_report",
		Payload:    data.ReportPayload{From: input.From, To: input.To},
	}
	if err := app.models.Jobs.Insert(job); err != nil {
		if errors.Is(err, data.ErrRecordNotFound) {
			app.notFoundResponse(w, r)
			return
		}
		app.serverErrorResponse(w, r, err)
		return
	}

	// The response contract: return a 202 Accepted response with a Location header pointing to the job status endpoint.
	// The client can poll this endpoint to check the status of the job.

	statusURL := fmt.Sprintf("/v1/jobs/%s", job.PublicID)
	headers := make(http.Header)
	headers.Set("Location", statusURL)
	response := envelope{"job_id": job.PublicID, "status": job.Status, "status_url": statusURL}
	// 202 Accepted, not 200/201: the request has been accepted for processing, but the processing has not been completed.
	// The client should check the Location URL to see when the job is done.
	if err := app.writeJSON(w, http.StatusAccepted, response, headers); err != nil {
		app.serverErrorResponse(w, r, err)
	}
}

// 202 Accepted, not 200/201:
// state, it only reports whatever the worker has most recently written for
// this job. Repeated calls to this handler are how a client observes a job
// moving through queued -> processing -> completed/failed.
func (app *application) getJobHandler(w http.ResponseWriter, r *http.Request) {
	job, err := app.models.Jobs.GetByPublicID(r.PathValue("id"))
	if err != nil {
		if errors.Is(err, data.ErrRecordNotFound) {
			app.notFoundResponse(w, r)
			return
		}
		app.serverErrorResponse(w, r, err)
		return
	}
	if err := app.writeJSON(w, http.StatusOK, envelope{"job": job}, nil); err != nil {
		app.serverErrorResponse(w, r, err)
	}
}
