package db

import "github.com/lib/pq"

func isForeignKeyViolationError(err error) bool {
	pgErr, valid := err.(*pq.Error)
	if !valid {
		return false
	}

	return pgErr.Code.Name() == "foreign_key_violation"
}
