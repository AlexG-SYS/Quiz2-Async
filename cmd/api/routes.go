package main

import "net/http"

// routes the three-endpoint async contract:
// 1. POST /v1/reports to create a new report job
// 2. GET /v1/jobs/{id} to check the status of a report job
// 3. GET /v1/healthcheck to check the health of the HTTP server itself

func (app *application) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/healthcheck", app.healthcheckHandler)
	mux.HandleFunc("POST /v1/consumers", app.createConsumersHandler)
	mux.HandleFunc("POST /v1/reports", app.createReportHandler)
	mux.HandleFunc("GET /v1/jobs/{id}", app.getJobHandler) // {id} here is the job's PublicID, not its internal DB id
	return mux
}
