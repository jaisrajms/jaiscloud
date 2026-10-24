package managedkafkaui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the Managed Kafka UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/clusters", h.ListClusters)
	r.Post("/clusters", h.CreateCluster)
	r.Get("/clusters/{location}/{cluster}", h.GetCluster)
	r.Put("/clusters/{location}/{cluster}", h.UpdateCluster)

	r.Get("/clusters/{location}/{cluster}/topics", h.ListClusterTopics)
	r.Post("/clusters/{location}/{cluster}/topics", h.CreateTopic)
	r.Get("/clusters/{location}/{cluster}/topics/{topic}", h.GetTopic)
	r.Put("/clusters/{location}/{cluster}/topics/{topic}", h.UpdateTopic)
	r.Delete("/clusters/{location}/{cluster}/topics/{topic}", h.DeleteTopic)

	r.Get("/clusters/{location}/{cluster}/acls", h.ListAcls)
	r.Post("/clusters/{location}/{cluster}/acls", h.CreateAcl)
	r.Put("/clusters/{location}/{cluster}/acls/{acl}", h.UpdateAcl)
	r.Delete("/clusters/{location}/{cluster}/acls/{acl}", h.DeleteAcl)
	r.Post("/clusters/{location}/{cluster}/acls/{acl}/entries", h.AddAclEntry)
	r.Delete("/clusters/{location}/{cluster}/acls/{acl}/entries", h.RemoveAclEntry)

	r.Get("/clusters/{location}/{cluster}/consumer-groups", h.ListConsumerGroups)
	r.Put("/clusters/{location}/{cluster}/consumer-groups/{group}", h.UpdateConsumerGroup)
	r.Delete("/clusters/{location}/{cluster}/consumer-groups/{group}", h.DeleteConsumerGroup)

	r.Get("/topics", h.ListTopics)

	return r
}
