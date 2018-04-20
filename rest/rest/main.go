package rest

import (
	"net/http"

	"github.com/gorilla/mux"
)

// Server ...
type Server struct {
	Router *mux.Router
}

// Init ...
func (s *Server) Init() {
	s.Router = mux.NewRouter()

	s.Router.HandleFunc("/command/{command}", s.Handle)
	s.Router.HandleFunc("/update/{id}", s.Handle)

	http.Handle("/", s.Router)
}

// Handle ...
func (s *Server) Handle(w http.ResponseWriter, r *http.Request) {

}
