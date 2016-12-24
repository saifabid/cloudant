package cloudant

import (
	"encoding/json"
	"fmt"

	"github.com/parnurzeal/gorequest"
)

// DB defines the parameters needed to make API calls against a specific database
type DB struct {
	Username string
	Password string
	Database string
	Host     string
}

// Query defines the parameters needed to make a request against cloudant query
type Query struct {
	Selector interface{}
	Fields   []string
	Sort     []map[string]string
	Limit    int
	Skip     int
}

// The set of constants defined here are for the various parameters
// allowed to be given to cloudant query
const (
	GreaterThan = "$gt"
	LessThan    = "$lt"
	Equal       = "$eq"
	Asc         = "asc"
)

// Setup inits all the params needed to make further requests to the cloudant API
func Setup(username, password, database, host string) *DB {
	return &DB{
		Username: username,
		Password: password,
		Database: database,
		Host:     host,
	}
}

func (db *DB) newRequest() *gorequest.SuperAgent {
	return gorequest.New().SetBasicAuth(db.Username, db.Password)
}

// Insert inserts a doccument
func (db *DB) Insert(doc interface{}) (string, error) {
	url := fmt.Sprintf("%s/%s", db.Host, db.Database)
	req := db.newRequest()
	resp, _, errs := req.Post(url).SendStruct(doc).End()
	if errs != nil {
		return "", errs[0]
	}

	type respJSON struct {
		Rev string `json:"_rev"`
	}

	var respBody respJSON
	err := json.NewDecoder(resp.Body).Decode(&respBody)
	if err != nil {
		return "", err
	}

	return respBody.Rev, nil
}
