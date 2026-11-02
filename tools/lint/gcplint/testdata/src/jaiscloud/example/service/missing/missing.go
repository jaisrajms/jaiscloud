package missing

import "context"

// Service mutates state but has no Reset(context.Context) hook.
type Service struct{}

func (s *Service) CreateThing() {}

func (s *Service) ReadThing(context.Context) {}
