package daos

type Action int

const (
	Create Action = iota
	Drop
	Update
)
