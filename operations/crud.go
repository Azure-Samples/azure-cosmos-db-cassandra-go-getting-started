package operations

import (
	"fmt"
	"log"

	"github.com/abhirockzz/gocass/model"
	"github.com/gocql/gocql"
)

const (
	createQuery       = "INSERT INTO %s.%s (user_id, user_name , user_bcity) VALUES (?,?,?)"
	selectQuery       = "SELECT * FROM %s.%s where user_id = ?"
	findAllUsersQuery = "SELECT * FROM %s.%s"
)

// InsertUser creates an entry(row) in a table
func InsertUser(keyspace, table string, session *gocql.Session, user model.User) {

	err := session.Query(fmt.Sprintf(createQuery, keyspace, table)).Bind(user.ID, user.Name, user.City).Exec()
	if err != nil {
		log.Fatal("Failed to create user", err)
	}
	log.Println("User created")
}

// FindUser tries to find a specific user
func FindUser(keyspace, table string, id int, session *gocql.Session) model.User {
	var userid int
	var name, city string
	err := session.Query(fmt.Sprintf(selectQuery, keyspace, table)).Bind(id).Scan(&userid, &name, &city)

	if err != nil {
		if err == gocql.ErrNotFound {
			log.Printf("User with id %v does not exist\n", id)
		} else {
			log.Printf("Failed to find user with id %v - %v\n", id, err)
		}
	}
	return model.User{ID: userid, Name: name, City: city}
}

// FindAllUsers gets all users
func FindAllUsers(keyspace, table string, session *gocql.Session) []model.User {

	var users []model.User
	results, _ := session.Query(fmt.Sprintf(findAllUsersQuery, keyspace, table)).Iter().SliceMap()

	for _, u := range results {
		users = append(users, mapToUser(u))
	}
	return users
}

func mapToUser(m map[string]interface{}) model.User {
	id, _ := m["user_id"].(int)
	name, _ := m["user_name"].(string)
	city, _ := m["user_bcity"].(string)

	return model.User{ID: id, Name: name, City: city}
}
