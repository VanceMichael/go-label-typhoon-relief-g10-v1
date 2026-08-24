package httpapi

import (
	"encoding/json"
	"net/http"
	"strings"
)

type JSONError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeNoContent(w http.ResponseWriter) { w.WriteHeader(http.StatusNoContent) }
func pathID(path, prefix string) string    { return strings.Trim(strings.TrimPrefix(path, prefix), "/") }
func contentType(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
}
func decodeStrict(r *http.Request, dst any) error {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	return decoder.Decode(dst)
}
