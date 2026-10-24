package tasksui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the Cloud Tasks UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/queues", h.ListQueues)
	r.Post("/queues", h.CreateQueue)
	r.Get("/queues/{location}/{queue}", h.GetQueue)
	r.Put("/queues/{location}/{queue}", h.UpdateQueue)
	r.Delete("/queues/{location}/{queue}", h.DeleteQueue)
	r.Post("/queues/{location}/{queue}/pause", h.PauseQueue)
	r.Post("/queues/{location}/{queue}/resume", h.ResumeQueue)
	r.Post("/queues/{location}/{queue}/purge", h.PurgeQueue)
	r.Get("/queues/{location}/{queue}/iam", h.GetQueueIam)
	r.Put("/queues/{location}/{queue}/iam", h.SetQueueIam)
	r.Get("/queues/{location}/{queue}/tasks", h.ListTasks)
	r.Post("/queues/{location}/{queue}/tasks", h.CreateTask)
	r.Get("/queues/{location}/{queue}/tasks/{task}", h.GetTask)
	r.Delete("/queues/{location}/{queue}/tasks/{task}", h.DeleteTask)
	r.Post("/queues/{location}/{queue}/tasks/{task}/run", h.RunTask)

	return r
}
