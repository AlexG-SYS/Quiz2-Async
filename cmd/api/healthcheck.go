package main

import (
	"net/http"
)

// healthcheckHandler is a liveness probe unrelated to the job system — it
// only confirms the HTTP server itself is up and answering, not that the
// database or worker are healthy.
func (app *application) healthcheckHandler(w http.ResponseWriter, r *http.Request) {
	env := envelope{
		"status": "available",
		"system_info": map[string]string{
			"environment": app.config.env,
		},
	}

	err := app.writeJSON(w, http.StatusOK, env, nil)
	if err != nil {
		app.serverErrorResponse(w, r, err)
	}
}
