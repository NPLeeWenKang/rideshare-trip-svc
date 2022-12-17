package main

import (
	"database/sql"
)

type Trip struct {
	Trip_Id      int          `json:"trip_id"`
	Passanger_Id int          `json:"passanger_id"`
	Pick_Up      string       `json:"pick_up"`
	Drop_Off     string       `json:"drop_off"`
	Start        sql.NullTime `json:"start"`
	End          sql.NullTime `json:"end"`
}

type Trip_Filter_Passanger struct {
	Trip_Id      int            `json:"trip_id"`
	Passanger_Id int            `json:"passanger_id"`
	Pick_Up      string         `json:"pick_up"`
	Drop_Off     string         `json:"drop_off"`
	Start        sql.NullTime   `json:"start"`
	End          sql.NullTime   `json:"end"`
	Status       sql.NullString `json:"status"`
}

func getTrip() ([]Trip, error) {
	dList := make([]Trip, 0)
	var rows *sql.Rows
	var err error

	rows, err = db.Query("SELECT * FROM trip")

	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var data Trip
		if err := rows.Scan(&data.Trip_Id, &data.Passanger_Id, &data.Pick_Up, &data.Drop_Off, &data.Start, &data.End); err != nil {
			return nil, err
		}
		dList = append(dList, data)
	}
	return dList, nil
}

func getTripFilterId(id *int) ([]Trip, error) {
	dList := make([]Trip, 0)
	var rows *sql.Rows
	var err error

	rows, err = db.Query("SELECT * FROM trip WHERE trip_id = ?", *id)

	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var data Trip
		if err := rows.Scan(&data.Trip_Id, &data.Passanger_Id, &data.Pick_Up, &data.Drop_Off, &data.Start, &data.End); err != nil {
			return nil, err
		}
		dList = append(dList, data)
	}
	return dList, nil
}

func getTripFilterPassangerId(passangerId string) ([]Trip_Filter_Passanger, error) {
	dList := make([]Trip_Filter_Passanger, 0)
	var rows *sql.Rows
	var err error

	rows, err = db.Query("WITH latest_assignment AS ( SELECT ta1.* FROM trip_assignment ta1 LEFT JOIN trip_assignment ta2 ON ta1.trip_id = ta2.trip_id AND ta1.assign_datetime < ta2.assign_datetime WHERE ta2.trip_id is NULL ), latest_trip AS ( SELECT t.trip_id, la.status FROM trip t LEFT JOIN latest_assignment la ON t.trip_id = la.trip_id ) SELECT t.*, lt.status FROM passanger p INNER JOIN trip t ON p.passanger_id = t.passanger_id INNER JOIN latest_trip lt ON t.trip_id = lt.trip_id WHERE p.passanger_id = ? ORDER BY t.trip_id DESC", passangerId)

	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var data Trip_Filter_Passanger
		if err := rows.Scan(&data.Trip_Id, &data.Passanger_Id, &data.Pick_Up, &data.Drop_Off, &data.Start, &data.End, &data.Status); err != nil {
			return nil, err
		}
		dList = append(dList, data)
	}
	return dList, nil
}

func insertTrip(t Trip) error {
	_, err := db.Query("INSERT INTO trip(trip_id, passanger_id, pick_up, drop_off, start, end) VALUES (?, ?, ?, ?, ?, ?)", t.Trip_Id, t.Passanger_Id, t.Pick_Up, t.Drop_Off, t.Start, t.End)
	return err
}

func updateTrip(id int, t Trip) error {
	_, err := db.Query("UPDATE trip SET passanger_id = ?, pick_up = ?, drop_off = ?, start = ?, end = ? WHERE trip_id = ?", t.Passanger_Id, t.Pick_Up, t.Drop_Off, t.Start, t.End, id)
	return err
}
