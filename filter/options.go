package filter

type Nest struct {
}

type Option func(*Converter)

func WithNestedJSONB(column string, exemption ...string) Option {
	return func(c *Converter) {
		c.nestedColumn = column
		c.nestedExemptions = exemption
	}
}
