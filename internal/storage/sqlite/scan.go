package sqlite

import (
	"database/sql"
	"fmt"
	"time"
)

type Scanner interface{ Scan(...any) error }

func ScanStorm(row Scanner) (id, org, name, status, severity string, version int, opened, closed time.Time, err error) {
	var o, c string
	err = row.Scan(&id, &org, &name, &status, &severity, &version, &o, &c)
	opened = TimeOrZero(o)
	closed = TimeOrZero(c)
	return
}
func ScanShelter(row Scanner) (id, org, name, address, status string, capacity, reserved, version int, err error) {
	err = row.Scan(&id, &org, &name, &address, &status, &capacity, &reserved, &version)
	return
}
func ScanTask(row Scanner) (id, storm, zone, title, status, owner, report string, priority, version int, lease, created, completed time.Time, err error) {
	var l, c, d string
	err = row.Scan(&id, &storm, &zone, &title, &status, &priority, &owner, &l, &version, &report, &c, &d)
	lease = TimeOrZero(l)
	created = TimeOrZero(c)
	completed = TimeOrZero(d)
	return
}
func ScanForecast(row Scanner) (id, storm, source, warning string, version int, wind float64, valid, published time.Time, err error) {
	var v, p string
	err = row.Scan(&id, &storm, &version, &source, &wind, &warning, &v, &p)
	valid = TimeOrZero(v)
	published = TimeOrZero(p)
	return
}
func CheckRows(rows *sql.Rows) error {
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows: %w", err)
	}
	return nil
}
