package repo

import (
	"math/rand"
	"time"

	"github.com/jmoiron/sqlx"
)

func getDBConnString() string {
	return "host=localhost port=5432 user=user password=pass dbname=kafka_yt sslmode=disable"
}

func NewDBConn() (*sqlx.DB, error) {
	db, err := sqlx.Connect("postgres", getDBConnString())
	if err != nil {
		return nil, err
	}
	return db, nil
}

var seededRand *rand.Rand = rand.New(
	rand.NewSource(time.Now().UnixNano()))

var charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func GenerateRandomString(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}
