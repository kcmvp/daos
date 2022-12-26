package daos

import (
	"time"
)

type Cache interface {
	Set(k, v string) error
	SetWithTtl(k, v string, ttl time.Duration) error
	Get(k string) (string, error)
	Ttl(k string) (time.Duration, error)
	Del(k string)
	CreateJsonIndex(name, pattern string, paths ...string) error
	DropIndex(name string) error
	Search(index string, by string) (map[string]string, error)
}
