package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/audit"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/auth"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/config"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/domain"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/evacuation"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/outbox"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/query"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/shelter"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/storage/sqlite"
	"github.com/VanceMichael/go-base-typhoon-relief-g10/internal/storm"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

type Server struct {
	DB         *sqlite.DB
	Config     config.Config
	Logger     *slog.Logger
	Auth       *auth.Service
	Storm      *storm.Service
	Shelter    *shelter.Service
	Evacuation *evacuation.Service
	Query      *query.Service
	Audit      *audit.Service
	Outbox     *outbox.Service
}

func New(db *sqlite.DB, c config.Config, l *slog.Logger) *Server {
	a := audit.New(db)
	o := outbox.New(db)
	return &Server{DB: db, Config: c, Logger: l, Auth: auth.New(db, c.SessionTTL), Storm: storm.New(db, a, o), Shelter: shelter.New(db, a, o), Evacuation: evacuation.New(db, a, o), Query: query.New(db), Audit: a, Outbox: o}
}

type userKey struct{}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.health)
	mux.HandleFunc("/readyz", s.ready)
	mux.HandleFunc("/v1/auth/login", s.login)
	mux.Handle("/v1/", s.withAuth(http.HandlerFunc(s.api)))
	return mux
}
func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, map[string]string{"status": "ok"})
}
func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.DB.Ready(ctx); err != nil {
		writeError(w, 503, err, "database not ready")
		return
	}
	writeJSON(w, 200, map[string]string{"status": "ready"})
}
func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	var in struct{ Email, Password string }
	if !decode(w, r, &in) {
		return
	}
	result, err := s.Auth.Login(r.Context(), in.Email, in.Password)
	if err != nil {
		writeError(w, 401, err, "credentials rejected")
		return
	}
	writeJSON(w, 200, result)
}
func (s *Server) withAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		u, err := s.Auth.Authenticate(r.Context(), token)
		if err != nil {
			writeError(w, 401, err, "authentication required")
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), userKey{}, u)))
	})
}
func current(ctx context.Context) domain.User { u, _ := ctx.Value(userKey{}).(domain.User); return u }
func (s *Server) api(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/")
	switch {
	case r.Method == http.MethodPost && path == "storms":
		s.createStorm(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(path, "storms/") && strings.HasSuffix(path, "/overview"):
		s.overview(w, r)
	case r.Method == http.MethodPost && path == "shelters":
		s.createShelter(w, r)
	case r.Method == http.MethodPost && strings.HasSuffix(path, "/activate"):
		s.activateStorm(w, r)
	default:
		writeError(w, 404, domain.ErrNotFound, "route not found")
	}
}
func (s *Server) createStorm(w http.ResponseWriter, r *http.Request) {
	var in struct{ Name, Severity string }
	if !decode(w, r, &in) {
		return
	}
	u := current(r.Context())
	item, err := s.Storm.Create(r.Context(), storm.CreateInput{OrganizationID: u.OrganizationID, Name: in.Name, Severity: in.Severity, ActorID: u.ID, RequestID: r.Header.Get("X-Request-ID")})
	if err != nil {
		writeMapped(w, err)
		return
	}
	writeJSON(w, 201, item)
}
func (s *Server) overview(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/storms/"), "/overview")
	if id == "" || strings.Contains(id, "/") {
		writeError(w, 400, domain.ErrValidation, "storm id required")
		return
	}
	item, err := s.Query.Overview(r.Context(), id)
	if err != nil {
		writeMapped(w, err)
		return
	}
	writeJSON(w, 200, item)
}
func (s *Server) createShelter(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Name, Address string
		Capacity      int
	}
	if !decode(w, r, &in) {
		return
	}
	u := current(r.Context())
	item, err := s.Shelter.Create(r.Context(), shelter.CreateInput{OrganizationID: u.OrganizationID, Name: in.Name, Address: in.Address, Capacity: in.Capacity, ActorID: u.ID, RequestID: r.Header.Get("X-Request-ID")})
	if err != nil {
		writeMapped(w, err)
		return
	}
	writeJSON(w, 201, item)
}
func (s *Server) activateStorm(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/storms/"), "/activate")
	u := current(r.Context())
	if err := s.Storm.Activate(r.Context(), id, u.ID, r.Header.Get("X-Request-ID")); err != nil {
		writeMapped(w, err)
		return
	}
	w.WriteHeader(204)
}
func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		writeError(w, 400, domain.ErrValidation, "invalid JSON")
		return false
	}
	return true
}
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
func writeError(w http.ResponseWriter, code int, err error, msg string) {
	writeJSON(w, code, map[string]any{"error": map[string]any{"code": codeName(code), "message": msg, "cause": err.Error()}})
}
func writeMapped(w http.ResponseWriter, err error) {
	code := 500
	switch {
	case errors.Is(err, domain.ErrUnauthorized):
		code = 401
	case errors.Is(err, domain.ErrForbidden):
		code = 403
	case errors.Is(err, domain.ErrNotFound):
		code = 404
	case errors.Is(err, domain.ErrConflict), errors.Is(err, domain.ErrInvalidState):
		code = 409
	case errors.Is(err, domain.ErrValidation):
		code = 422
	}
	writeError(w, code, err, "business operation failed")
}
func codeName(code int) string {
	switch code {
	case 401:
		return "unauthorized"
	case 403:
		return "forbidden"
	case 404:
		return "not_found"
	case 409:
		return "conflict"
	case 422:
		return "validation"
	case 503:
		return "unavailable"
	}
	return "internal_error"
}
