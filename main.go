package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"

	"github.com/go-sql-driver/mysql"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
)

var db *sql.DB

var cfg = mysql.Config{
	User:      "user",
	Passwd:    "password",
	Net:       "tcp",
	Addr:      "localhost:3306",
	DBName:    "db",
	ParseTime: true,
}

func main() {
	db, _ = sql.Open("mysql", cfg.FormatDSN())
	defer db.Close()

	allowOrigins := handlers.AllowedOrigins([]string{"*"})
	allowMethods := handlers.AllowedMethods([]string{"GET", "POST", "OPTIONS", "DELETE", "PUT"})
	allowHeaders := handlers.AllowedHeaders([]string{"X-Requested-With", "Content-Type"})

	router := mux.NewRouter()
	router.HandleFunc("/api/v1/current_trip_assignment/passenger/{id}", currentAssignmentPassenger).Methods(http.MethodGet)

	router.HandleFunc("/api/v1/current_trip_assignment/driver/{id}", currentAssignmentDriver).Methods(http.MethodGet)

	router.HandleFunc("/api/v1/trip/{id}", filterTrip).Methods(http.MethodGet, http.MethodPut)
	router.HandleFunc("/api/v1/trip", trip).Methods(http.MethodGet, http.MethodPost)

	router.HandleFunc("/api/v1/trip_assignment", tripAssignment).Methods(http.MethodPut)

	fmt.Println("Listening at port 5001")
	log.Fatal(http.ListenAndServe(":5001", handlers.CORS(allowOrigins, allowMethods, allowHeaders)(router)))
}

func trip(w http.ResponseWriter, r *http.Request) {
	querystringmap := r.URL.Query()
	passengerId := querystringmap["passenger_id"]

	switch r.Method {
	case http.MethodGet:
		if len(passengerId) >= 1 {
			tList, err := getTripFilterPassengerId(passengerId[0])
			if err == nil {
				w.WriteHeader(http.StatusAccepted)
				out, _ := json.Marshal(tList)
				w.Header().Set("Content-type", "application/json")
				fmt.Fprintf(w, string(out))
			} else {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, err.Error())
			}
		} else {
			tList, err := getTrip()
			if err == nil {
				w.WriteHeader(http.StatusAccepted)
				out, _ := json.Marshal(tList)
				w.Header().Set("Content-type", "application/json")
				fmt.Fprintf(w, string(out))
			} else {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, err.Error())
			}
		}

	case http.MethodPost:
		var t Trip
		if byteBody, ok := ioutil.ReadAll(r.Body); ok == nil {
			if ok := json.Unmarshal(byteBody, &t); ok == nil {
				err := insertTrip(t)
				if err == nil {
					w.WriteHeader(http.StatusAccepted)
					w.Header().Set("Content-type", "application/json")
					fmt.Fprintf(w, "Inserted Trip Id %d", t.Trip_Id)
				} else {
					w.WriteHeader(http.StatusBadRequest)
					fmt.Fprintf(w, err.Error())
				}
			}
		}

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		fmt.Fprintf(w, "Error")
	}
}
func filterTrip(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	if _, ok := params["id"]; !ok {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "No ID")
	}
	id, _ := strconv.Atoi(params["id"])

	switch r.Method {
	case http.MethodGet:

		tList, err := getTripFilterId(&id)
		if err == nil {
			w.WriteHeader(http.StatusAccepted)
			out, _ := json.Marshal(tList)
			w.Header().Set("Content-type", "application/json")
			fmt.Fprintf(w, string(out))
		} else {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, err.Error())
		}

	case http.MethodPut:
		var t Trip
		if byteBody, ok := ioutil.ReadAll(r.Body); ok == nil {
			if ok := json.Unmarshal(byteBody, &t); ok == nil {
				err := updateTrip(id, t)
				if err == nil {
					w.WriteHeader(http.StatusAccepted)
					w.Header().Set("Content-type", "application/json")
					fmt.Fprintf(w, "Updated Trip Id %d", t.Trip_Id)
				} else {
					w.WriteHeader(http.StatusBadRequest)
					fmt.Fprintf(w, err.Error())
				}
			}
		}
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		fmt.Fprintf(w, "Error")
	}
}

func currentAssignmentPassenger(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	if _, ok := params["id"]; !ok {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "No ID")
	}
	id, _ := strconv.Atoi(params["id"])

	switch r.Method {
	case http.MethodGet:
		tList, err := getCurrentTripAssignmentFilterPassengerId(id)
		if err == nil {
			w.WriteHeader(http.StatusAccepted)
			out, _ := json.Marshal(tList)
			w.Header().Set("Content-type", "application/json")
			fmt.Fprintf(w, string(out))
		} else {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, err.Error())
		}

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		fmt.Fprintf(w, "Error")
	}
}

func currentAssignmentDriver(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	if _, ok := params["id"]; !ok {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "No ID")
	}
	id, _ := strconv.Atoi(params["id"])

	switch r.Method {
	case http.MethodGet:
		tList, err := getCurrentTripAssignmentFilterDriverId(id)
		if err == nil {
			w.WriteHeader(http.StatusAccepted)
			out, _ := json.Marshal(tList)
			w.Header().Set("Content-type", "application/json")
			fmt.Fprintf(w, string(out))
		} else {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, err.Error())
		}

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		fmt.Fprintf(w, "Error")
	}
}

func tripAssignment(w http.ResponseWriter, r *http.Request) {

	switch r.Method {
	case http.MethodPut:
		var ta Trip_Assignment
		if byteBody, ok := ioutil.ReadAll(r.Body); ok == nil {
			if ok := json.Unmarshal(byteBody, &ta); ok == nil {
				var err error
				if ta.Status == "ACCEPTED" || ta.Status == "REJECTED" {
					err = updateTripAssignment(ta)
				} else if ta.Status == "DRIVING" {
					fmt.Println(ta)
					err = updateTripAssignmentAndTripStart(ta)
				} else if ta.Status == "DONE" {
					err = updateTripAssignmentAndTripEnd(ta)
				}
				if err == nil {
					w.WriteHeader(http.StatusAccepted)
					w.Header().Set("Content-type", "application/json")
					fmt.Fprintf(w, "trip_id %d driver_id %d updated", ta.Trip_Id, ta.Driver_Id)
				} else {
					w.WriteHeader(http.StatusBadRequest)
					fmt.Fprintf(w, err.Error())
				}
			}
		}
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		fmt.Fprintf(w, "Error")
	}
}
