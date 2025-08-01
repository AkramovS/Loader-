package db

import (
	"Loader/models"
	"context"
	"github.com/jackc/pgx/v5"
	"log"
)

// Добавляем в таблицу Payments
func InsertPayment(conn *pgx.Conn, p models.Payment) error {
	_, err := conn.Exec(context.Background(),
		`INSERT INTO payments (file_name, payment_system, payment_id, amount, account_number, payment_datetime, uploaded_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		p.FileName,
		p.PaymentSystem,
		p.PaymentID,
		p.Amount,
		p.AccountNumber,
		p.PaymentDateTime,
		p.UploadedAt,
	)
	if err != nil {
		log.Println("Ошибка вставки платежа: %v", err)
	}
	return err
}

func CreateTable(conn *pgx.Conn) error {
	_, err := conn.Exec(context.Background(),
		`
	CREATE TABLE IF NOT EXISTS payments (
		id               SERIAL PRIMARY KEY,
		file_name        TEXT           NOT NULL,
		payment_system   TEXT,
		payment_id       TEXT UNIQUE,
		amount           NUMERIC(12, 2) NOT NULL,
		account_number   TEXT           NOT NULL,
		payment_datetime TIMESTAMP      NOT NULL,
		uploaded_at      TIMESTAMP DEFAULT now()
	);`)

	return err
}
