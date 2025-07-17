package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"log"
	"strconv"
	"time"

	"github.com/xuri/excelize/v2"
)

type Payment struct {
	AccountNumber   string
	PaymentDateTime time.Time
	Amount          float64
}

func main() {
	// Подключение к PostgreSQL
	conn, err := pgx.Connect(context.Background(), "postgres://postgres:Akramchik938747405@localhost:5432/payments")
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}
	defer conn.Close(context.Background())

	// Открываем Excel-файл
	f, err := excelize.OpenFile("Алиф1.xlsx")
	if err != nil {
		log.Fatalf("Failed to open Excel file: %v", err)
	}

	// Читаем первую таблицу
	sheet := f.GetSheetName(0)
	rows, err := f.GetRows(sheet)
	if err != nil {
		log.Fatalf("Failed to read rows: %v", err)
	}

	// Пропускаем заголовок
	for i, row := range rows[1:] {
		if len(row) < 3 {
			log.Printf("Skipping incomplete row %d: %v", i+2, row)
			continue
		}

		account := row[0]

		// Парсим дату и время
		paymentTime, err := parseDateTime(row[1])
		if err != nil {
			log.Printf("Invalid datetime in row %d: %v", i+2, err)
			continue
		}

		// Парсим сумму
		amount, err := strconv.ParseFloat(row[2], 64)
		if err != nil {
			log.Printf("Invalid amount in row %d: %v", i+2, err)
			continue
		}

		payment := Payment{AccountNumber: account, PaymentDateTime: paymentTime, Amount: amount}

		// Загружаем в БД
		if err := insertPayment(conn, payment); err != nil {
			log.Printf("DB insert failed in row %d: %v", i+2, err)
		}
	}

	fmt.Println("Import completed.")
}

func parseDateTime(value string) (time.Time, error) {
	// Попытка разных форматов
	formats := []string{
		time.RFC3339,
		"02.01.2006 15:04",
		"2006-01-02 15:04:05",
		"02.01.2006",
	}
	for _, format := range formats {
		if t, err := time.Parse(format, value); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized date format: %s", value)
}

func insertPayment(conn *pgx.Conn, p Payment) error {
	_, err := conn.Exec(context.Background(),
		`INSERT INTO payments (account_number, payment_datetime, amount)
		 VALUES ($1, $2, $3)`,
		p.AccountNumber, p.PaymentDateTime, p.Amount)
	return err
}
