package daos

import (
	"time"
)

type index struct {
	Name    string   `json:"name"`
	Pattern string   `json:"pattern"`
	Paths   []string `json:"paths"`
	Version int64    `json:"version"`
}

type cmd int

const (
	set cmd = iota
	del
	ttl
)

type action struct {
	C cmd    `json:"c"`
	K string `json:"k"`
	V string `json:"v"`
	T int    `json:"t"`
}

type Cache interface {
	Set(k, v string, ttl time.Duration)
	Get(k string) string
	Del(k string) (string, error)
	CreateJsonIndex(name, pattern string, paths ...string) error
	DropIndex(name string) error
	SearchIndexBy(index string, by string) map[string]string
}
