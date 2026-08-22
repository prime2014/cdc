package streams

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
)

type PIIMode string

const (
	PIINone   PIIMode = "none"
	PIIRedact PIIMode = "redact"
	PIIHash   PIIMode = "hash"
)

type PIISanitizer struct {
	mode   PIIMode
	fields map[string]struct{}
	salt   string
}

func (s *PIISanitizer) Sanitize(event *CDCEvent) error {
	if s.mode == PIINone || len(s.fields) == 0 {
		return nil
	}

	var err error
	if event.Before != nil {
		event.Before, err = s.sanitizeJSON(event.Before)

		if err != nil {
			return err
		}
	}

	if event.After != nil {
		event.After, err = s.sanitizeJSON(event.After)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *PIISanitizer) sanitizeJSON(raw json.RawMessage) (json.RawMessage, error) {
	var m map[string]any

	if err := json.Unmarshal(raw, &m); err != nil {
		return raw, err
	}

	for k, v := range m {
		if _, isPII := s.fields[strings.ToLower(k)]; !isPII {
			continue
		}

		m[k] = s.transform(v)
	}

	return json.Marshal(m)
}

func (s *PIISanitizer) transform(v any) any {
	str, ok := v.(string)

	if !ok {
		// non-string PII: still Redact
		if s.mode == PIIRedact {
			return "[REDACTED]"
		}
		return v
	}

	switch s.mode {
	case PIIRedact:
		return "[REDACTED]"
	case PIIHash:
		h := sha256.Sum256([]byte(s.salt + str))
		return hex.EncodeToString(h[:])
	default:
		return v
	}
}
