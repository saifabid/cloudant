package cloudant

import (
	"encoding/json"
	"fmt"
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

const (
	pkgErrorCode = 0
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

// Err defines the interface met by by API Errors or any other errors while using this package
type Err interface {
	Message() map[string]string
	StatusCode() int
}

// PkgError defines any errors which are not returned from cloudant
type PkgError struct {
	err error
}

// Message returns the error as a map
func (p PkgError) Message() map[string]string {
	return map[string]string{
		"error": p.err.Error(),
	}
}

// StatusCode returns a default status of 0 signifying its a package error and not an HTTP error
func (p PkgError) StatusCode() int {
	return pkgErrorCode
}

// APIError defines the params for a non 2xx response we get back from cloudant
type APIError struct {
	message        map[string]string
	httpStatusCode int
}

// Message returns the cloudant response body unmarhsaled into a map when the response is not 2xx
func (a APIError) Message() map[string]string {
	return a.message
}

// StatusCode returns the http status code returned by cloudant when the response is not 2xx
func (a APIError) StatusCode() int {
	return a.httpStatusCode
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
func (db *DB) Insert(doc interface{}) (string, Err) {
	url := fmt.Sprintf("%s/%s", db.Host, db.Database)
	req := db.newRequest()
	resp, body, errs := req.Post(url).SendStruct(doc).EndBytes()
	if errs != nil {
		return "", PkgError{
			err: errs[0],
		}
	}

	if resp.StatusCode/100 != 2 {
		var v map[string]string
		err := json.Unmarshal(body, &v)
		if err != nil {
			return "", PkgError{
				err: err,
			}
		}

		return "", APIError{
			httpStatusCode: resp.StatusCode,
			message:        v,
		}
	}

	type respJSON struct {
		Rev string `json:"rev"`
	}

	var respBody respJSON
	err := json.Unmarshal(body, &respBody)
	if err != nil {
		return "", PkgError{
			err: errs[0],
		}
	}

	return respBody.Rev, nil
}

// GetByID gets a single doccument by it's _id
func (db *DB) GetByID(id string, params map[string]string) ([]byte, Err) {
	url := fmt.Sprintf("%s/%s/%s?%s", db.Host, db.Database, id, mapToQueryString(params))
	req := db.newRequest()
	resp, body, errs := req.Get(url).EndBytes()
	if errs != nil {
		return nil, PkgError{
			err: errs[0],
		}
	}

	if resp.StatusCode%100 != 2 {
		var v map[string]string
		err := json.Unmarshal(body, &v)
		if err != nil {
			return nil, PkgError{
				err: err,
			}
		}

		return nil, APIError{
			httpStatusCode: resp.StatusCode,
			message:        v,
		}
	}

	return body, nil
}

// Update will update a single doccument with the new doccument and returns the rev of the doccument updated
func (db *DB) Update(id string, doc interface{}) (string, Err) {
	url := fmt.Sprintf("%s/%s/%s", db.Host, db.Database, id)
	req := db.newRequest()
	resp, body, errs := req.Put(url).SendStruct(doc).EndBytes()
	if errs != nil {
		return "", PkgError{
			err: errs[0],
		}
	}

	if resp.StatusCode/100 != 2 {
		var v map[string]string
		err := json.Unmarshal(body, &v)
		if err != nil {
			return "", PkgError{
				err: err,
			}
		}

		return "", APIError{
			httpStatusCode: resp.StatusCode,
			message:        v,
		}
	}

	type respJSON struct {
		Rev string `json:"rev"`
	}

	var respBody respJSON
	err := json.Unmarshal(body, &respBody)
	if err != nil {
		return "", PkgError{
			err: errs[0],
		}
	}

	return respBody.Rev, nil
}

// Delete will delete a doccument
func (db *DB) Delete(id, rev string) Err {
	url := fmt.Sprintf("%s/%s/%s?rev=%s", db.Host, db.Database, id, rev)
	req := db.newRequest()
	resp, body, errs := req.Delete(url).EndBytes()
	if errs != nil {
		return PkgError{
			err: errs[0],
		}
	}

	if resp.StatusCode/100 != 2 {
		var v map[string]string
		err := json.Unmarshal(body, &v)
		if err != nil {
			return PkgError{
				err: err,
			}
		}

		return APIError{
			httpStatusCode: resp.StatusCode,
			message:        v,
		}
	}

	return nil
}

func mapToQueryString(m map[string]string) string {
	var q string
	for k, v := range m {
		q = q + fmt.Sprintf("%s=%s&", k, v)
	}

	return strings.Trim(q, "&")
}
