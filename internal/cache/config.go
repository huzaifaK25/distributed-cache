package cache

import "time"

// Config controls cache capacity and background expiration cleanup.
type Config struct {
	// MaxItems is the maximum number of entries. Values <= 0 mean unlimited.
	MaxItems int

	// CleanupInterval is how often the background worker sweeps expired keys.
	// Values <= 0 disable the worker; Get still removes expired keys lazily.
	CleanupInterval time.Duration
}
