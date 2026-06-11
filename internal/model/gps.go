package model

import "time"

type GPSPayload struct {
	Lat  float64 `json:"lat"`
	Lng  float64 `json:"lng"`
	Spd  float64 `json:"spd"`
	Alt  float64 `json:"alt"`
	Sat  int     `json:"sat"`
	HDOP float64 `json:"hdop"`
	Fix  bool    `json:"fix"`
	Ts   int64   `json:"ts"`
}

type GPSEvent struct {
	BusID      string    `json:"bus_id"`
	Lat        float64   `json:"lat"`
	Lng        float64   `json:"lng"`
	SpeedKmh   float64   `json:"spd"`
	AltM       float64   `json:"alt"`
	Satellites int       `json:"sat"`
	HDOP       float64   `json:"hdop"`
	Fix        bool      `json:"fix"`
	ReceivedAt time.Time `json:"received_at"`
}