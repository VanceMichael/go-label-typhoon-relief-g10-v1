package query

import (
	"context"
	"encoding/csv"
	"io"
	"strconv"

	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/domain"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/storm"
)

func (s *Service) ExportTasks(ctx context.Context, stormID string, dst io.Writer) error {
	page, err := s.Tasks(ctx, stormID, TaskFilter{Limit: 100})
	if err != nil {
		return err
	}
	w := csv.NewWriter(dst)
	if err := w.Write([]string{"id", "title", "status", "priority", "owner"}); err != nil {
		return err
	}
	for _, t := range page.Items {
		if err := w.Write([]string{t.ID, t.Title, string(t.Status), strconv.Itoa(t.Priority), t.OwnerID}); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}
func (s *Service) StormPage(ctx context.Context, org string, limit, offset int) (domain.Page[domain.StormEvent], error) {
	return storm.NewRepository(s.DB).List(ctx, org, limit, offset)
}
