package sfnui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the Step Functions UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/state-machines", h.ListStateMachines)
	r.Post("/state-machines", h.CreateStateMachine)
	r.Delete("/state-machines/{arn}", h.DeleteStateMachine)
	r.Post("/state-machines/{arn}/executions", h.StartExecution)
	r.Get("/state-machines/{arn}/executions", h.ListExecutions)
	r.Post("/executions/{arn}/stop", h.StopExecution)
	r.Get("/executions/{arn}/history", h.GetExecutionHistory)

	return r
}
