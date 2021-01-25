package barriermq

// Message ...
type Message interface {
	Encode() []byte
	Decode([]byte)
}
