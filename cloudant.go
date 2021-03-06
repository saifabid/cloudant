package cloudant

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/parnurzeal/gorequest"
)

// The set of constants defined here are for the various parameters
// allowed to be given to cloudant query
const (
	GreaterThan = "$gt"
	LessThan    = "$lt"
	Equal       = "$eq"
	Asc         = "asc"
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

// Insert inserts a doccument and returns the rev of the doccument created
func (db *DB) Insert(doc interface{}) (string, error) {
	url := fmt.Sprintf("%s/%s", db.Host, db.Database)
	req := db.newRequest()
	resp, body, errs := req.Post(url).SendStruct(doc).EndBytes()
	if errs != nil {
		return "", errs[0]
	}

	if resp.StatusCode/100 != 2 {
		var v map[string]string
		err := json.Unmarshal(body, &v)
		if err != nil {
			return "", errs[0]
		}

		return "", errors.New(string(body))
	}

	type respJSON struct {
		Rev string `json:"rev"`
	}

	var respBody respJSON
	err := json.Unmarshal(body, &respBody)
	if err != nil {
		return "", err
	}

	return respBody.Rev, nil
}

// GetByID gets a single doccument by it's _id
func (db *DB) GetByID(id string, params map[string]interface{}) ([]byte, error) {
	url := fmt.Sprintf("%s/%s/%s?%s", db.Host, db.Database, id, mapToQueryString(params))
	req := db.newRequest()
	resp, body, errs := req.Get(url).EndBytes()
	if errs != nil {
		return nil, errs[0]
	}

	if resp.StatusCode/100 != 2 {
		var v map[string]string
		err := json.Unmarshal(body, &v)
		if err != nil {
			return nil, err
		}

		return nil, errors.New(string(body))
	}

	return body, nil
}

// Update will update a single doccument with the new doccument and returns the rev of the doccument updated
func (db *DB) Update(id string, doc interface{}) (string, error) {
	url := fmt.Sprintf("%s/%s/%s", db.Host, db.Database, id)
	req := db.newRequest()
	resp, body, errs := req.Put(url).SendStruct(doc).EndBytes()
	if errs != nil {
		return "", errs[0]
	}

	if resp.StatusCode/100 != 2 {
		var v map[string]string
		err := json.Unmarshal(body, &v)
		if err != nil {
			return "", err
		}

		return "", errors.New(string(body))
	}

	type respJSON struct {
		Rev string `json:"rev"`
	}

	var respBody respJSON
	err := json.Unmarshal(body, &respBody)
	if err != nil {
		return "", errs[0]
	}

	return respBody.Rev, nil
}

// Delete will delete a doccument
func (db *DB) Delete(id, rev string) error {
	url := fmt.Sprintf("%s/%s/%s?rev=%s", db.Host, db.Database, id, rev)
	req := db.newRequest()
	resp, body, errs := req.Delete(url).EndBytes()
	if errs != nil {
		return errs[0]
	}

	if resp.StatusCode/100 != 2 {
		var v map[string]string
		err := json.Unmarshal(body, &v)
		if err != nil {
			return err
		}

		return errors.New(string(body))
	}

	return nil
}

// Query performs a cloudant query call
func (db *DB) Query(params interface{}) ([]byte, error) {
	url := fmt.Sprintf("%s/%s/_find", db.Host, db.Database)
	req := db.newRequest()

	resp, body, errs := req.Post(url).SendStruct(params).EndBytes()
	if errs != nil {
		return nil, errs[0]
	}

	if resp.StatusCode/100 != 2 {
		var v map[string]string
		err := json.Unmarshal(body, &v)
		if err != nil {
			return nil, err
		}

		return nil, errors.New(string(body))
	}

	return body, nil
}

// View gets data from a view
func (db *DB) View(ddoc string, iName string, q map[string]interface{}) ([]byte, error) {
	url := fmt.Sprintf("%s/%s/_design/%s/_view/%s?%s", db.Host, db.Database, ddoc, iName, mapToQueryString(q))
	req := db.newRequest()

	resp, body, errs := req.Get(url).EndBytes()
	if errs != nil {
		return nil, errs[0]
	}

	if resp.StatusCode/100 != 2 {
		var v map[string]string
		err := json.Unmarshal(body, &v)
		if err != nil {
			return nil, err
		}

		return nil, errors.New(string(body))
	}

	return body, nil
}

// Search performs a lucene search
func (db *DB) Search(ddoc string, iName string, q map[string]interface{}) ([]byte, error) {
	url := fmt.Sprintf("%s/%s/_design/%s/_search/%s?%s", db.Host, db.Database, ddoc, iName, mapToQueryString(q))
	req := db.newRequest()

	resp, body, errs := req.Get(url).EndBytes()
	if errs != nil {
		return nil, errs[0]
	}

	if resp.StatusCode/100 != 2 {
		var v map[string]string
		err := json.Unmarshal(body, &v)
		if err != nil {
			return nil, err
		}

		return nil, errors.New(string(body))
	}

	return body, nil
}

func mapToQueryString(m map[string]interface{}) string {
	var q string
	for k, v := range m {
		switch v := v.(type) {
		case string:
			if v == "" {
				continue
			}
			q = q + fmt.Sprintf("%s=%s&", k, url.QueryEscape(v))
		case int32, int64:
			q = q + fmt.Sprintf("%s=%d&", k, v)
		case bool:
			q = q + fmt.Sprintf("%s=%t&", k, v)
		default:
			q = q + fmt.Sprintf("%s=%s&", k, v)
		}
	}

	return strings.Trim(q, "&")
}
