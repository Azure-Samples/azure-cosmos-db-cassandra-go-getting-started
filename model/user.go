package model

import (
	"fmt"
)

// User represents a user
type User struct {
	ID         int
	Name, City string
}

const userInfoFormat = "ID:%v, Name:%s, City:%s"

func (u User) String() string {
	return fmt.Sprintf(userInfoFormat, u.ID, u.Name, u.City)
}
