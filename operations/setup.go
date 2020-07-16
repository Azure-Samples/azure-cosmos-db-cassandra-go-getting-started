package operations

import (
	"fmt"
	"log"

	"github.com/Azure-Samples/azure-cosmos-db-cassandra-go-getting-started/utils"
	"github.com/gocql/gocql"
)

const (
	dropKeyspace   = "DROP KEYSPACE IF EXISTS %s"
	createKeyspace = "CREATE KEYSPACE %s WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1 }"
	createTable    = "CREATE TABLE %s.%s (user_id int PRIMARY KEY, user_name text, user_bcity text)"
)

// DropKeySpaceIfExists drops keyspace if it exists
func DropKeySpaceIfExists(keyspace string, session *gocql.Session) {
	err := utils.ExecuteQuery(fmt.Sprintf(dropKeyspace, keyspace), session)
	if err != nil {
		log.Fatal("Failed to drop keyspace", err)
	}
	log.Println("Keyspace dropped")
}

// CreateKeySpace creates a keyspace
func CreateKeySpace(keyspace string, session *gocql.Session) {

	err := utils.ExecuteQuery(fmt.Sprintf(createKeyspace, keyspace), session)
	if err != nil {
		log.Fatal("Failed to create keyspace", err)
	}
	log.Println("Keyspace created")
}

// CreateUserTable creates a table
func CreateUserTable(keyspace, table string, session *gocql.Session) {
	err := session.Query(fmt.Sprintf(createTable, keyspace, table)).Exec()
	if err != nil {
		log.Fatal("Failed to create table", err)
	}
	log.Println("Table created")
}
