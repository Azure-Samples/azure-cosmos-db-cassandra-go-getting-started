package main

import (
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/abhirockzz/gocass/model"
	"github.com/abhirockzz/gocass/operations"
	"github.com/abhirockzz/gocass/utils"
)

var (
	cosmosCassandraContactPoint string
	cosmosCassandraPort         string
	cosmosCassandraUser         string
	cosmosCassandraPassword     string
)

var cities = []string{"New Delhi", "New York", "Bangalore", "Seattle"}

const (
	keyspace = "user_profile"
	table    = "user"
)

func init() {
	cosmosCassandraContactPoint = os.Getenv("COSMOSDB_CASSANDRA_CONTACT_POINT")
	cosmosCassandraPort = os.Getenv("COSMOSDB_CASSANDRA_PORT")
	cosmosCassandraUser = os.Getenv("COSMOSDB_CASSANDRA_USER")
	cosmosCassandraPassword = os.Getenv("COSMOSDB_CASSANDRA_PASSWORD")

	if cosmosCassandraContactPoint == "" || cosmosCassandraUser == "" || cosmosCassandraPassword == "" {
		log.Fatal("missing mandatory environment variables")
	}
}

func main() {
	session := utils.GetSession(cosmosCassandraContactPoint, cosmosCassandraPort, cosmosCassandraUser, cosmosCassandraPassword)
	defer session.Close()

	log.Println("Connected to Azure Cosmos DB")

	operations.DropKeySpaceIfExists(keyspace, session)
	operations.CreateKeySpace(keyspace, session)
	time.Sleep(2 * time.Second)

	operations.CreateUserTable(keyspace, table, session)
	time.Sleep(2 * time.Second)

	for i := 1; i <= 5; i++ {
		name := "user-" + strconv.Itoa(i)
		operations.InsertUser(keyspace, table, session, model.User{ID: i, Name: name, City: cities[rand.Intn(len(cities))]})
	}
	time.Sleep(2 * time.Second)

	user := operations.FindUser(keyspace, table, 1, session)
	log.Println("Found User ", user)
	time.Sleep(2 * time.Second)

	users := operations.FindAllUsers(keyspace, table, session)
	log.Println("Found Users ", users)
}
