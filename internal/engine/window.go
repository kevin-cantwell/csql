package engine

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// AttachInfo describes a SQLite database file to ATTACH to each window.
type AttachInfo struct {
	Schema string // e.g., "_src_users"
	Path   string // file path to ATTACH
}

// Window is a time-partitioned SQLite database.
type Window struct {
	DB           *sql.DB
	Start        time.Time
	End          time.Time
	insertedKeys map[string]map[interface{}]bool // table → set of inserted join keys
}

// hasKey returns true if the given key has already been inserted for the given table.
func (w *Window) hasKey(table string, key interface{}) bool {
	if w.insertedKeys == nil {
		return false
	}
	keys, ok := w.insertedKeys[table]
	if !ok {
		return false
	}
	return keys[key]
}

// markKey records that a key has been inserted for the given table.
func (w *Window) markKey(table string, key interface{}) {
	if w.insertedKeys == nil {
		w.insertedKeys = make(map[string]map[interface{}]bool)
	}
	keys, ok := w.insertedKeys[table]
	if !ok {
		keys = make(map[interface{}]bool)
		w.insertedKeys[table] = keys
	}
	keys[key] = true
}

// WindowManager manages tumbling time windows.
type WindowManager struct {
	duration     time.Duration
	staticTables map[string]bool
	staticDB     *sql.DB
	attachments  []AttachInfo
	mu           sync.Mutex
	windows      []*Window
}

// NewWindowManager creates a new window manager.
func NewWindowManager(duration time.Duration, staticTables map[string]bool, staticDB *sql.DB, attachments []AttachInfo) *WindowManager {
	return &WindowManager{
		duration:     duration,
		staticTables: staticTables,
		staticDB:     staticDB,
		attachments:  attachments,
	}
}

// Current returns the current time window, creating it if necessary and expiring old ones.
func (wm *WindowManager) Current() (*Window, error) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	now := time.Now()
	windowStart := now.Truncate(wm.duration)

	// Check if the current window matches
	if len(wm.windows) > 0 {
		last := wm.windows[len(wm.windows)-1]
		if last.Start.Equal(windowStart) {
			return last, nil
		}
	}

	// Create new window
	win, err := wm.createWindow(windowStart)
	if err != nil {
		return nil, err
	}

	wm.windows = append(wm.windows, win)

	// Keep at most 2 windows (current + previous)
	for len(wm.windows) > 2 {
		old := wm.windows[0]
		old.DB.Close()
		wm.windows = wm.windows[1:]
	}

	return win, nil
}

func (wm *WindowManager) createWindow(start time.Time) (*Window, error) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("create window db: %w", err)
	}

	// Attach static DB if we have one
	if wm.staticDB != nil {
		_, err = db.Exec("ATTACH DATABASE 'file:static?mode=memory&cache=shared' AS static")
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("attach static db: %w", err)
		}
	}

	// Attach SQLite source databases
	for _, att := range wm.attachments {
		_, err = db.Exec(fmt.Sprintf(
			"ATTACH DATABASE %s AS %s",
			quoteLiteral(att.Path), quoteIdent(att.Schema)))
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("attach %s: %w", att.Schema, err)
		}
	}

	return &Window{
		DB:    db,
		Start: start,
		End:   start.Add(wm.duration),
	}, nil
}

// Close closes all windows.
func (wm *WindowManager) Close() {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	for _, w := range wm.windows {
		w.DB.Close()
	}
	wm.windows = nil
}
