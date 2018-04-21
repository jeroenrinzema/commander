package rest

import (
	"encoding/json"
	"net/http"
	"reflect"
)

// Request create a new request struct
// that can be used after initalizing to preform actions on the
// http request.
type Request struct {
	Request        *http.Request
	ResponseWriter http.ResponseWriter
}

// GetJSONBody get and decode the JSON body of a request
func (r *Request) GetJSONBody(model interface{}) {
	decoder := json.NewDecoder(r.Request.Body)
	decoder.Decode(model)
}

// GetParams get and collect the url parameters of an request
// all parameters are being stored in the struct as a string.
func (r *Request) GetParams(model interface{}) {
	r.Request.ParseForm()
	form := r.Request.Form

	v := reflect.ValueOf(model).Elem()
	t := v.Type()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		key := field.Name
		paramTag := field.Tag.Get("param")

		if len(paramTag) != 0 {
			key = paramTag
		}

		v.Field(i).SetString(form.Get(key))
	}
}
