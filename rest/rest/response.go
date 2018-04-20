package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"gopkg.in/go-playground/validator.v9"
)

// Response ...
type Response struct {
	ResponseWriter http.ResponseWriter
	Status         int
	Message        string
	Body           interface{}
}

const (
	successMessage        = "Success"
	createdMessage        = "Created"
	notImplementedMessage = "Not Implemented"
	notFoundMessage       = "Not Found"
	noContentMessage      = "No Content"
	notAllowedMessage     = "Not Allowed"
)

// SendOK send a 200 "OK" back with a custom body
// The const successMessage will be used as meta message
func (r *Response) SendOK(body interface{}) {
	setJSON(r.ResponseWriter)
	setHTTPStatus(r, http.StatusOK)
	setHTTPMessage(r, successMessage)
	setBody(r, body)
}

// SendCreated send a 201 "Created" back with the given body
// The const successMessage will be used as meta message
func (r *Response) SendCreated(body interface{}) {
	setJSON(r.ResponseWriter)
	setHTTPStatus(r, http.StatusCreated)
	setHTTPMessage(r, createdMessage)
	setBody(r, body)
}

// SendNoContent send a 501 "Not Implemented" back
// The const notImplementedMessage will be used as meta message
func (r *Response) SendNoContent() {
	setJSON(r.ResponseWriter)
	setHTTPStatus(r, http.StatusNoContent)
	setHTTPMessage(r, noContentMessage)
	setBody(r, nil)
}

// SendBadRequest send a 400 "Bad Request" back
// The given message will be used in the meta field
func (r *Response) SendBadRequest(message string) {
	setJSON(r.ResponseWriter)
	setHTTPStatus(r, http.StatusBadRequest)
	setHTTPMessage(r, message)
	setBody(r, nil)
}

// SendNotFound send a 404 "Not Found" back
// The const notFoundMessage will be used as meta message
func (r *Response) SendNotFound() {
	setJSON(r.ResponseWriter)
	setHTTPStatus(r, http.StatusNotFound)
	setHTTPMessage(r, notFoundMessage)
	setBody(r, nil)
}

// SendNotImplemented send a 501 "Not Implemented" back
// The const notImplementedMessage will be used as meta message
func (r *Response) SendNotImplemented() {
	setJSON(r.ResponseWriter)
	setHTTPStatus(r, http.StatusNotImplemented)
	setHTTPMessage(r, notImplementedMessage)
	setBody(r, nil)
}

// SendStatus send a custom status with empty message back
// The body of the response will be empty
func (r *Response) SendStatus(status int) {
	setJSON(r.ResponseWriter)
	setHTTPStatus(r, status)
	setBody(r, nil)
}

// SendStatusWithMessage send a custom status and message back
// The body of the response will be empty
func (r *Response) SendStatusWithMessage(status int, message string) {
	setJSON(r.ResponseWriter)
	setHTTPStatus(r, status)
	setHTTPMessage(r, message)
	setBody(r, nil)
}

// SendPanic send a 500 "Internal Server Error" back with a status message
// The given message will be used in the meta field
func (r *Response) SendPanic(message string) {
	setJSON(r.ResponseWriter)
	setHTTPStatus(r, http.StatusInternalServerError)
	setHTTPMessage(r, message)
	setBody(r, nil)
}

// SendNotAllowed send a 405 "Method Not Allowed" back with an empty body
// The const 'notAllowedMessage' will be used as meta message
func (r *Response) SendNotAllowed() {
	setJSON(r.ResponseWriter)
	setHTTPStatus(r, http.StatusMethodNotAllowed)
	setHTTPMessage(r, notAllowedMessage)
	setBody(r, nil)
}

// SendInvalidBody send a 500 "Internal Server Error" back with an empty body
// The meta message will contain information about the missing or invalid field(s)
func (r *Response) SendInvalidBody(errs validator.ValidationErrors) {
	var fields []string
	for _, err := range errs {
		fields = append(fields, err.Field())
	}

	setJSON(r.ResponseWriter)
	setHTTPStatus(r, http.StatusInternalServerError)
	setHTTPMessage(r, fmt.Sprintf("The following fields are missing or invalid: %s", strings.Join(fields, ",")))
	setBody(r, nil)
}

////////////////////////////////

type responseMeta struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type responseBody struct {
	Meta responseMeta `json:"meta"`
	Data interface{}  `json:"data"`
}

func setHTTPStatus(r *Response, status int) {
	r.ResponseWriter.WriteHeader(status)
	r.Status = status
}

func setHTTPMessage(r *Response, message string) {
	r.Message = message
}

func setJSON(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
}

func setBody(r *Response, body interface{}) {
	r.Body = responseBody{responseMeta{r.Status, r.Message}, body}
	json.NewEncoder(r.ResponseWriter).Encode(r.Body)
}
