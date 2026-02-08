package source

import (
	"fmt"
	"strings"
)

// Record is a single row from a source: column names to values.
type Record map[string]interface{}

// SourceType classifies sources as streaming or static.
type SourceType int

const (
	Streaming SourceType = iota
	Static
)

// Source reads records from a data source.
type Source interface {
	// Type returns whether this is a streaming or static source.
	Type() SourceType
	// Name returns the table name for this source.
	Name() string
	// Records returns a channel of records. For static sources, the channel closes
	// after all records are sent. For streaming sources, it stays open until ctx is done.
	Records() (<-chan Record, error)
	// Close cleans up resources.
	Close() error
}

// Config describes a source from a --source flag.
type Config struct {
	Name   string
	URI    string
	Scheme string
	Table  string // optional: source table name (for sqlite, defaults to Name)
}

// ParseURI parses a source URI like "file://path.csv", "sqlite://path.db?table=t", or "stdin".
func ParseURI(name, uri string) (*Config, error) {
	if uri == "stdin" || uri == "" {
		return &Config{Name: name, URI: uri, Scheme: "stdin"}, nil
	}
	if len(uri) > 7 && uri[:7] == "file://" {
		return &Config{Name: name, URI: uri[7:], Scheme: "file"}, nil
	}
	if strings.HasPrefix(uri, "sqlite://") {
		raw := uri[len("sqlite://"):]
		// Strip leading slash for absolute paths (sqlite:///abs/path -> /abs/path)
		// but keep it for relative paths (sqlite://rel/path -> rel/path)
		path := raw
		table := ""
		if idx := strings.IndexByte(path, '?'); idx >= 0 {
			query := path[idx+1:]
			path = path[:idx]
			for _, kv := range strings.Split(query, "&") {
				parts := strings.SplitN(kv, "=", 2)
				if len(parts) == 2 && parts[0] == "table" {
					table = parts[1]
				}
			}
		}
		cfg := &Config{Name: name, URI: path, Scheme: "sqlite"}
		cfg.Table = table
		return cfg, nil
	}
	if len(uri) > 8 && uri[:8] == "mysql://" {
		return &Config{Name: name, URI: uri, Scheme: "mysql"}, nil
	}
	if len(uri) > 11 && uri[:11] == "postgres://" {
		return &Config{Name: name, URI: uri, Scheme: "postgres"}, nil
	}
	// Default: treat as file path
	return &Config{Name: name, URI: uri, Scheme: "file"}, nil
}

// NewSource creates a source from a config.
func NewSource(cfg *Config) (Source, error) {
	switch cfg.Scheme {
	case "stdin":
		return NewStdinSource(cfg.Name), nil
	case "file":
		return NewFileSource(cfg.Name, cfg.URI)
	case "sqlite":
		return NewSQLiteSource(cfg.Name, cfg.URI, cfg.Table)
	default:
		return nil, fmt.Errorf("unsupported source scheme: %s", cfg.Scheme)
	}
}
