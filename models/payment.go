package models

import "time"

type Payment struct {
	FileName        string
	PaymentSystem   string
	PaymentID       string
	Amount          float64
	AccountNumber   string
	PaymentDateTime time.Time
	UploadedAt      time.Time
}
