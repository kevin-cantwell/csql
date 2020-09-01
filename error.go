package csql

import "errors"

var (
	ErrNoStreamSpecified         = errors.New("No stream specified")
	ErrStreamDoesNotExist        = errors.New("Stream does not exist")
	ErrStreamAlreadyExists       = errors.New("Stream already exists")
	ErrIndexAlreadyExists        = errors.New("Index already exists")
	ErrViolatesUniqueConstraint  = errors.New("Duplicate key value violates unique constraint")
	ErrViolatesNotNullConstraint = errors.New("Value violates not null constraint")
	ErrColumnDoesNotExist        = errors.New("Column does not exist")
	ErrInvalidSelectItem         = errors.New("Select item is not valid")
	ErrInvalidDatatype           = errors.New("Invalid datatype")
	ErrMissingValues             = errors.New("Missing values")
	ErrInvalidCell               = errors.New("Cell is invalid")
	ErrInvalidOperands           = errors.New("Operands are invalid")
	ErrPrimaryKeyAlreadyExists   = errors.New("Primary key already exists")
)
