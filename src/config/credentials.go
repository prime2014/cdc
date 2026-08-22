package config

import (
	"fmt"
	"net/url"
	"os"
	"strings"
)

func LoadDatabaseURL() (string, error) {
	// Full URL provided
	if raw := os.Getenv("DATABASE_URL"); raw != "" {
		return ensureReplicationParam(raw)
	}

	// Build from parts
	user := os.Getenv("PG_USER")
	pass := os.Getenv("PG_PASSWORD")
	host := os.Getenv("PG_HOST")
	port := os.Getenv("PG_PORT")
	db := os.Getenv("PG_DB")
	ssl := os.Getenv("PG_SSL_MODE")

	if user == "" || pass == "" || host == "" || db == "" {
		return "", fmt.Errorf("database credentials not provided (set DATABASE_URL or PG* + PASSWORD)")
	}

	if port == "" {
		port = "5432"
	}

	if ssl == "" {
		ssl = "disable"
	}

	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=%s&replication=database",
		user, pass, host, port, db, ssl,
	), nil
}

// normalize the postgres url to ensure it has the required parameters
func ensureReplicationParam(raw string) (string, error) {
	u, err := url.Parse(raw)

	if err != nil {
		return "", fmt.Errorf("invalid DATABASE_URL: %w", err)
	}

	q := u.Query()
	if strings.EqualFold(q.Get("replication"), "database") {
		return raw, nil
	}

	q.Set("replication", "database")
	q.Set("sslmode", "disable")
	u.RawQuery = q.Encode()
	return u.String(), nil
}
