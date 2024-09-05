package log

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	log_v1 "github.com/Lucas/api/v1"
	"github.com/Lucas/log"

	"github.com/gorilla/mux"
)

type Server struct {
	*mux.Router
	log        *log.Log
	segment    *log.Segment
	baseOffset uint64
	nextOffset uint64
	config     log.Config
}

func NewServer() *Server {
	dir := "segments"
	baseOffset := uint64(0)
	c := log.Config{
		Segment: struct {
			MaxStoresBytes uint64
			MaxIndexBytes  uint64
			InitialOffset  uint64
		}{
			MaxStoresBytes: 1024 * 1024, MaxIndexBytes: 1024 * 1024, InitialOffset: 0,
		},
	}

	s := &Server{
		Router:     mux.NewRouter(),
		baseOffset: baseOffset,
		config:     c,
	}

	segment, err := log.NewSegment(dir, baseOffset, c)
	if err != nil {
		fmt.Printf("Error al crear el segmento: %v\n", err)
	}
	s.segment = segment

	s.routes()

	return s
}

func (s *Server) routes() {
	s.HandleFunc("/record", s.append()).Methods("POST")
	s.HandleFunc("/record/{offset}", s.get()).Methods("GET")
}

func (s *Server) append() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var rec log_v1.Record

		if err := json.NewDecoder(r.Body).Decode(&rec); err != nil {
			http.Error(w, "Error al decodificar", http.StatusBadRequest)
			return
		}

		off, err := s.segment.Append(&rec)
		if err != nil {
			http.Error(w, "Error al agregar el registro", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
		fmt.Fprintf(w, "Registro creado exitosamente: %d", off)
	}
}

func (s *Server) get() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		offsetStr := vars["offset"]
		offset, err := strconv.ParseUint(offsetStr, 10, 64)
		if err != nil {
			http.Error(w, "Offset invalido", http.StatusBadRequest)
			return
		}

		rec, err := s.segment.Read(offset)
		if err != nil {
			http.Error(w, "Error al leer el registro", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "applocation/json")
		if err := json.NewEncoder(w).Encode(rec); err != nil {
			http.Error(w, "Error al codificar", http.StatusInternalServerError)
			return
		}
	}
}
