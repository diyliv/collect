package interfaces

type Producer interface {
	Produce(messages []string) error
}
