package app

import "context"

type Service struct{}

func (s *Service) Reset(context.Context) {}

func (s *Service) CreateThing() {}
