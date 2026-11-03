package server

import (
	"fmt"

	"distributed-cache/internal/cache"
)

// Protocol responses.
const (
	responsePong     = "PONG"
	responseOK       = "OK"
	responseNotFound = "NOT_FOUND"
	responseBusy     = "ERR server busy"
	responseTooLarge = "ERR request too large"
	responseInternal = "ERR internal error"

	responseTooManyConnections = "ERR too many connections"
)

// Handler maps parsed commands to cache operations.
//
// It is safe for concurrent use when the supplied cache is safe for concurrent
// use, which InMemoryCache is.
type Handler struct {
	cache cache.Cache
}

// NewHandler returns a Handler backed by c.
func NewHandler(c cache.Cache) *Handler {
	if c == nil {
		panic("server: nil cache")
	}
	return &Handler{cache: c}
}

// Handle executes cmd and returns a single-line protocol response.
func (h *Handler) Handle(cmd Command) string {
	switch cmd.Type {
	case CommandPing:
		return responsePong

	case CommandSet:
		if err := h.cache.Set(cmd.Key, cmd.Value); err != nil {
			return "ERR set failed: " + err.Error()
		}
		return responseOK

	case CommandGet:
		value, found := h.cache.Get(cmd.Key)
		if !found {
			return responseNotFound
		}
		return fmt.Sprint(value)

	case CommandDel:
		if err := h.cache.Delete(cmd.Key); err != nil {
			return "ERR delete failed: " + err.Error()
		}
		return responseOK

	case CommandStats:
		stats := h.cache.Stats()
		return fmt.Sprintf(
			"hits=%d misses=%d sets=%d deletes=%d expired=%d evictions=%d",
			stats.Hits, stats.Misses, stats.Sets, stats.Deletes,
			stats.Expired, stats.Evictions,
		)

	default:
		return fmt.Sprintf("ERR unknown command %q", cmd.Type)
	}
}
