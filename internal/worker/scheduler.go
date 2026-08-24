package worker

import (
	"context"
	"sync"
	"time"
)

type Task func(context.Context) error
type Scheduler struct {
	mu      sync.Mutex
	tasks   map[string]Task
	running map[string]bool
}

func NewScheduler() *Scheduler {
	return &Scheduler{tasks: map[string]Task{}, running: map[string]bool{}}
}
func (s *Scheduler) Register(name string, t Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if name == "" || t == nil {
		return ErrInvalidTask
	}
	if _, ok := s.tasks[name]; ok {
		return ErrDuplicateTask
	}
	s.tasks[name] = t
	return nil
}
func (s *Scheduler) Run(ctx context.Context, name string) error {
	s.mu.Lock()
	t, ok := s.tasks[name]
	if !ok {
		s.mu.Unlock()
		return ErrMissingTask
	}
	if s.running[name] {
		s.mu.Unlock()
		return ErrTaskRunning
	}
	s.running[name] = true
	s.mu.Unlock()
	defer func() { s.mu.Lock(); delete(s.running, name); s.mu.Unlock() }()
	return t(ctx)
}
func (s *Scheduler) RunPeriodic(ctx context.Context, name string, interval time.Duration) {
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = s.Run(ctx, name)
		}
	}
}
func (s *Scheduler) Names() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.tasks))
	for name := range s.tasks {
		out = append(out, name)
	}
	return out
}
