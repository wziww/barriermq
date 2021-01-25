package barriermq

var (
	stateOpen  state = 0b01
	stateClose state = 0b10
)

// Queue ...
type Queue interface {
	Consumer() <-chan interface{}
}

// Handler message handler func
type Handler func(interface{}) error
