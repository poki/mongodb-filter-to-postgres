package filter

import "fmt"

// ErrNoAccessOption is returned when no access options are provided to NewConverter.
var ErrNoAccessOption = fmt.Errorf("NewConverter: need atleast one of the access options: WithAllowAllColumns, WithAllowColumns, WithNestedJSONB")

type ColumnNotAllowedError struct {
	Column string
}

func (e ColumnNotAllowedError) Error() string {
	return fmt.Sprintf("column not allowed: %s", e.Column)
}
