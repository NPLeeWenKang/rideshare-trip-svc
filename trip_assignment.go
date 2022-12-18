package main

import (
	"context"
	"database/sql"
)

type Trip_Assignment struct {
	Trip_Id   int    `json:"trip_id"`
	Driver_Id int    `json:"driver_id"`
	Status    string `json:"status"`
}

type Trip_Assignment_With_Passenger_Trip struct {
	Trip_Id      int            `json:"trip_id"`
	Driver_Id    sql.NullInt32  `json:"driver_id"`
	Status       sql.NullString `json:"status"`
	Passenger_Id int            `json:"passenger_id"`
	First_Name   sql.NullString `json:"first_name"`
	Last_Name    sql.NullString `json:"last_name"`
	Mobile_No    sql.NullString `json:"mobile_no"`
	Email        sql.NullString `json:"email"`
	Pick_Up      string         `json:"pick_up"`
	Drop_Off     string         `json:"drop_off"`
	Start        sql.NullTime   `json:"start"`
	End          sql.NullTime   `json:"end"`
	Car_No       sql.NullString `json:"car_no"`
}

type Trip_Assignment_With_Driver_Trip struct {
	Trip_Id      int          `json:"trip_id"`
	Driver_Id    int          `json:"driver_id"`
	Status       string       `json:"status"`
	Passenger_Id int          `json:"passenger_id"`
	First_Name   string       `json:"first_name"`
	Last_Name    string       `json:"last_name"`
	Mobile_No    string       `json:"mobile_no"`
	Email        string       `json:"email"`
	Pick_Up      string       `json:"pick_up"`
	Drop_Off     string       `json:"drop_off"`
	Start        sql.NullTime `json:"start"`
	End          sql.NullTime `json:"end"`
}

// Gets the "in-progress" trip assignments for the drivers
func getCurrentTripAssignmentFilterDriverId(driverId int) ([]Trip_Assignment_With_Driver_Trip, error) {
	dList := make([]Trip_Assignment_With_Driver_Trip, 0)
	var rows *sql.Rows
	var err error

	// Filters out the most updated trip_assignment for any trip and get the ones that are still in-progress. Also includes those trips that have not been assigned even once (will cause driver details to be null).
	rows, err = db.Query("WITH latest_assignment AS ( SELECT ta1.* FROM trip_assignment ta1 LEFT JOIN trip_assignment ta2 ON ta1.trip_id = ta2.trip_id AND ta1.assign_datetime < ta2.assign_datetime WHERE ta2.trip_id is NULL AND ta1.status != 'DONE' AND ta1.status != 'REJECTED' ) SELECT d.driver_id, t.*, la.status, p.first_name, p.last_name, p.mobile_no, p.email  FROM latest_assignment la INNER JOIN trip t ON la.trip_id = t.trip_id INNER JOIN passenger p ON t.passenger_id = p.passenger_id INNER JOIN driver d ON d.driver_id = la.driver_id WHERE d.driver_id = ?", driverId)

	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var data Trip_Assignment_With_Driver_Trip
		if err := rows.Scan(&data.Driver_Id, &data.Trip_Id, &data.Passenger_Id, &data.Pick_Up, &data.Drop_Off, &data.Start, &data.End, &data.Status, &data.First_Name, &data.Last_Name, &data.Mobile_No, &data.Email); err != nil {
			return nil, err
		}
		dList = append(dList, data)
	}
	return dList, nil
}

// Gets the "in-progress" trip assignments for the drivers
func getCurrentTripAssignmentFilterPassengerId(passengerId int) ([]Trip_Assignment_With_Passenger_Trip, error) {
	dList := make([]Trip_Assignment_With_Passenger_Trip, 0)
	var rows *sql.Rows
	var err error

	// Filters out the most updated trip_assignment for any trip and get the ones that are still in-progress.
	rows, err = db.Query("WITH latest_assignment AS ( SELECT ta1.* FROM trip_assignment ta1 LEFT JOIN trip_assignment ta2 ON ta1.trip_id = ta2.trip_id AND ta1.assign_datetime < ta2.assign_datetime WHERE ta2.trip_id is NULL ), notnull_trip AS ( SELECT t.trip_id, la.driver_id, la.status, la.assign_datetime FROM trip t LEFT JOIN latest_assignment la ON t.trip_id = la.trip_id WHERE la.status != 'DONE' ), null_trip AS ( SELECT t.trip_id, la.driver_id, la.status, la.assign_datetime FROM trip t LEFT JOIN latest_assignment la ON t.trip_id = la.trip_id WHERE la.status IS NULL ), latest_trip AS ( SELECT * FROM notnull_trip UNION SELECT * FROM null_trip ) SELECT d.driver_id, t.*, lt.status, d.first_name, d.last_name, d.mobile_no, d.email, d.car_no  FROM latest_trip lt INNER JOIN trip t ON lt.trip_id = t.trip_id INNER JOIN passenger p ON t.passenger_id = p.passenger_id LEFT JOIN driver d ON d.driver_id = lt.driver_id WHERE p.passenger_id = ?", passengerId)

	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var data Trip_Assignment_With_Passenger_Trip
		if err := rows.Scan(&data.Driver_Id, &data.Trip_Id, &data.Passenger_Id, &data.Pick_Up, &data.Drop_Off, &data.Start, &data.End, &data.Status, &data.First_Name, &data.Last_Name, &data.Mobile_No, &data.Email, &data.Car_No); err != nil {
			return nil, err
		}
		dList = append(dList, data)
	}
	return dList, nil
}

func updateTripAssignment(t Trip_Assignment) error {
	_, err := db.Query("UPDATE trip_assignment SET status = ? WHERE trip_id = ? AND driver_id = ?", t.Status, t.Trip_Id, t.Driver_Id)
	return err
}

// Since multiple update statements are used, a transaction is used.
func updateTripAssignmentAndTripStart(t Trip_Assignment) error {
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE trip_assignment SET status = ? WHERE trip_id = ? AND driver_id = ?", t.Status, t.Trip_Id, t.Driver_Id)
	if err != nil {
		tx.Rollback()
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE trip SET start = CURRENT_TIMESTAMP WHERE trip_id = ?", t.Trip_Id)
	if err != nil {
		tx.Rollback()
		return err
	}
	err = tx.Commit()
	return err
}

// Since multiple update statements are used, a transaction is used.
func updateTripAssignmentAndTripEnd(t Trip_Assignment) error {
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE trip_assignment SET status = ? WHERE trip_id = ? AND driver_id = ?", t.Status, t.Trip_Id, t.Driver_Id)
	if err != nil {
		tx.Rollback()
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE trip SET end = CURRENT_TIMESTAMP WHERE trip_id = ?", t.Trip_Id)
	if err != nil {
		tx.Rollback()
		return err
	}
	err = tx.Commit()
	return err
}
