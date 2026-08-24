package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

type QueryBuilder struct {
	table               string
	columns, predicates []string
	args                []any
	order               string
	limit, offset       int
}

func Select(table string, columns ...string) *QueryBuilder {
	return &QueryBuilder{table: table, columns: append([]string(nil), columns...), limit: -1}
}
func (q *QueryBuilder) Where(predicate string, args ...any) *QueryBuilder {
	if strings.TrimSpace(predicate) != "" {
		q.predicates = append(q.predicates, predicate)
		q.args = append(q.args, args...)
	}
	return q
}
func (q *QueryBuilder) OrderBy(order string) *QueryBuilder { q.order = order; return q }
func (q *QueryBuilder) Page(limit, offset int) *QueryBuilder {
	if limit < 0 {
		limit = 0
	}
	if offset < 0 {
		offset = 0
	}
	q.limit = limit
	q.offset = offset
	return q
}
func (q *QueryBuilder) SQL() (string, []any, error) {
	if q.table == "" || len(q.columns) == 0 {
		return "", nil, fmt.Errorf("table and columns required")
	}
	query := "SELECT " + strings.Join(q.columns, ",") + " FROM " + q.table
	if len(q.predicates) > 0 {
		query += " WHERE " + strings.Join(q.predicates, " AND ")
	}
	if q.order != "" {
		query += " ORDER BY " + q.order
	}
	if q.limit >= 0 {
		query += fmt.Sprintf(" LIMIT %d OFFSET %d", q.limit, q.offset)
	}
	return query, append([]any(nil), q.args...), nil
}
func (q *QueryBuilder) Run(ctx context.Context, db *DB) (*sql.Rows, error) {
	query, args, err := q.SQL()
	if err != nil {
		return nil, err
	}
	return db.QueryContext(ctx, query, args...)
}
