package lang_parser

type Token interface {
	GetKind() TokenKind
	GetInt() int64
	GetFloat() float64
	GetBool() bool
	GetSymbol() string
	String() string
}
