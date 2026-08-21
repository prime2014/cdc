package streams

import (
	"io"
	"os"
	"strings"

	"github.com/jackc/pglogrepl"
)

type CheckpointStore struct {
	file *os.File
}

func NewCheckpointStore(path string) (*CheckpointStore, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		return nil, err
	}

	return &CheckpointStore{file: f}, nil
}

func (c *CheckpointStore) Load() (pglogrepl.LSN, error) {
	// Move to the beginning of the file
	if _, err := c.file.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	data, err := io.ReadAll(c.file)
	if err != nil {
		return 0, err
	}

	if len(data) == 0 {
		return 0, nil
	}

	return pglogrepl.ParseLSN(strings.TrimSpace(string(data)))
}

func (c *CheckpointStore) Save(lsn pglogrepl.LSN) error {
	if _, err := c.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	if err := c.file.Truncate(0); err != nil {
		return err
	}

	_, err := c.file.WriteString(lsn.String() + "\n")

	if err != nil {
		return err
	}

	// Force the data to disk (important for crash safety)
	return c.file.Sync()
}

func (c *CheckpointStore) Close() error {
	return c.file.Close()
}
