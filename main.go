package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/xuri/excelize/v2"
	"log"
	"path/filepath"
	"strconv"
	"time"
)

type Payment struct {
	AccountNumber   string
	PaymentDateTime time.Time
	Amount          float64
}

func main() {

	// Подключение к PostgreSQL
	conn, err := pgx.Connect(context.Background(), "postgres://postgres:Akramchik938747405@localhost:5432/payments?sslmode=disable")
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}
	defer conn.Close(context.Background())

	// Получаем все файлы .xlsx из папки data/
	files, err := filepath.Glob("data/*.xlsx")
	if err != nil {
		log.Fatalf("Failed to find Excel files: %v", err)
	}
	if len(files) == 0 {
		log.Println("No Excel files found in ./data")
		return
	}

	for _, file := range files {
		fmt.Printf("📄 Обработка файла: %s\n", file)
		if err := processFile(file, conn); err != nil {
			log.Printf(" Ошибка в файле %s: %v", file, err)
		}
	}

	fmt.Println(" Загрузка завершена.")
}

func processFile(path string, conn *pgx.Conn) error {
	// Открываем Excel-файл
	f, err := excelize.OpenFile(path)
	if err != nil {
		return fmt.Errorf("не удалось открыть файл: %w", err)
	}
	defer f.Close()

	// Читаем первую таблицу
	sheet := f.GetSheetName(0)
	rows, err := f.GetRows(sheet)
	if err != nil {
		return fmt.Errorf("не удалось прочитать строки: %w", err)
	}

	// Пропускаем заголовок
	for i, row := range rows[1:] {
		if len(row) < 3 {
			log.Printf(" Пропущена неполная строка %d: %v", i+2, row)
			continue
		}

		account := row[0]

		// Парсим дату и время
		paymentTime, err := parseDateTime(row[1])
		if err != nil {
			log.Printf(" Ошибка даты в строке %d: %v", i+2, err)
			continue
		}

		// Парсим сумму
		amount, err := strconv.ParseFloat(row[2], 64)
		if err != nil {
			log.Printf(" Ошибка суммы в строке %d: %v", i+2, err)
			continue
		}

		payment := Payment{AccountNumber: account, PaymentDateTime: paymentTime, Amount: amount}

		// Загружаем в БД
		if err := insertPayment(conn, payment); err != nil {
			log.Printf(" Ошибка вставки в БД (строка %d): %v", i+2, err)
		}
	}

	return nil
}

func parseDateTime(value string) (time.Time, error) {
	// Попытка разных форматов даты
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
	return time.Time{}, fmt.Errorf("неизвестный формат даты: %s", value)
}

func insertPayment(conn *pgx.Conn, p Payment) error {
	_, err := conn.Exec(context.Background(),
		`INSERT INTO payments (account_number, payment_datetime, amount)
		 VALUES ($1, $2, $3)`,
		p.AccountNumber, p.PaymentDateTime, p.Amount)
	return err
}
