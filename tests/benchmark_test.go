package tests

import (
	"strconv"
	"testing"

	"distributed-cache/internal/cache"
)

// benchmarkKeyCount limits the key space so Set benchmarks reuse keys instead
// of measuring a constantly growing map.
const benchmarkKeyCount = 1 << 14

// benchmarkKeys builds the keys before timing starts to isolate cache cost from
// string formatting cost.
func benchmarkKeys() []string {
	keys := make([]string, benchmarkKeyCount)
	for i := range keys {
		keys[i] = "key-" + strconv.Itoa(i)
	}
	return keys
}

// benchmarkValues pre-boxes values so Set benchmarks measure cache work rather
// than interface conversion allocations.
func benchmarkValues(n int) []any {
	values := make([]any, n)
	for i := range values {
		values[i] = i
	}
	return values
}

// BenchmarkSet measures insert/update cost with preallocated keys and values.
func BenchmarkSet(b *testing.B) {
	c := cache.New()
	keys := benchmarkKeys()
	values := benchmarkValues(len(keys))

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := c.Set(keys[i%len(keys)], values[i%len(values)]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGet measures read cost against a fully populated cache.
func BenchmarkGet(b *testing.B) {
	c := cache.New()
	keys := benchmarkKeys()
	for i, key := range keys {
		if err := c.Set(key, i); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, found := c.Get(keys[i%len(keys)]); !found {
			b.Fatalf("key %q missing", keys[i%len(keys)])
		}
	}
}

// BenchmarkMixedReadWrite measures a 90% read / 10% write workload.
func BenchmarkMixedReadWrite(b *testing.B) {
	c := cache.New()
	keys := benchmarkKeys()
	values := benchmarkValues(len(keys))
	for i, key := range keys {
		if err := c.Set(key, values[i]); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := keys[i%len(keys)]

		// Every tenth operation is a write; the rest are reads.
		if i%10 == 0 {
			if err := c.Set(key, values[i%len(values)]); err != nil {
				b.Fatal(err)
			}
			continue
		}

		if _, found := c.Get(key); !found {
			b.Fatalf("key %q missing", key)
		}
	}
}
