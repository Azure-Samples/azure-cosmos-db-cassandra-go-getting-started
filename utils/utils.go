package utils

import (
	"context"
	"crypto/tls"
	"log"
	"strconv"
	"time"

	"github.com/gocql/gocql"
)

// GetSession connects to Cassandra
func GetSession(cosmosCassandraContactPoint, cosmosCassandraPort, cosmosCassandraUser, cosmosCassandraPassword string) *gocql.Session {
	clusterConfig := gocql.NewCluster(cosmosCassandraContactPoint)
	port, err := strconv.Atoi(cosmosCassandraPort)
	if err != nil {
		log.Fatal(err)
	}
	clusterConfig.Port = port
	clusterConfig.ProtoVersion = 4
	clusterConfig.Authenticator = gocql.PasswordAuthenticator{Username: cosmosCassandraUser, Password: cosmosCassandraPassword}
	clusterConfig.SslOpts = &gocql.SslOptions{Config: &tls.Config{MinVersion: tls.VersionTLS12}}

	clusterConfig.Timeout = 3 * time.Second
	//clusterConfig.QueryObserver = timer{}
	session, err := clusterConfig.CreateSession()
	if err != nil {
		log.Fatal("Failed to connect to Azure Cosmos DB", err)
	}

	return session
}

// ExecuteQuery executes a query and returns an error if any
func ExecuteQuery(query string, session *gocql.Session) error {
	return session.Query(query).Exec()
}

type timer struct {
}

func (t timer) ObserveQuery(ctx context.Context, oq gocql.ObservedQuery) {
	log.Printf("Time taken for '%s' = %v ", oq.Statement, time.Since(oq.Start))
}
