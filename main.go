package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/xuri/excelize/v2"
	"log"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Payment struct {
	FileName        string
	PaymentSystem   string
	PaymentID       string
	Amount          float64
	AccountNumber   string
	PaymentDateTime time.Time
	UploadedAt      time.Time
}

func main() {
	// Подключение к PostgresSQL
	conn, err := pgx.Connect(context.Background(), "postgres://postgres:Akramchik938747405@localhost:5432/payments?sslmode=disable")
	if err != nil {
		log.Fatalf("Ошибка подключения к базе данных: %v", err)
	}
	defer conn.Close(context.Background())

	files, err := filepath.Glob("data/*.xlsx")
	if err != nil {
		log.Fatalf("Ошибка поиска Excel файлов: %v", err)
	}
	if len(files) == 0 {
		log.Println("Нет Excel файлов в папке ./data")
		return
	}

	for _, file := range files {
		fmt.Printf("Обработка файла: %s\n", file)
		if err := handleExcelFile(file, conn); err != nil {
			log.Printf("Ошибка в файле %s: %v", file, err)
		}
	}

	fmt.Println("Загрузка завершена.")
}

// Открываем все файлы в папке Data
func handleExcelFile(path string, conn *pgx.Conn) error {
	file, err := excelize.OpenFile(path)
	if err != nil {
		return fmt.Errorf("не удалось открыть файл: %w", err)
	}
	defer file.Close()

	switch {
	case strings.Contains(path, "Алиф"):
		return alifProcessFile(file, conn, path)
	case strings.Contains(path, "Зудамал"):
		return ZudamalProcessFile(file, conn, path)
	case strings.Contains(strings.ToLower(path), "международн"):
		return IbtProcessFile(file, conn, path)
	case strings.Contains(path, "Хумо"):
		return humoProcessFile(file, conn, path)
	case strings.Contains(path, "Шукр Молия"):
		return shukrProcessFile(file, conn, path)
	case strings.Contains(path, "Душанбе Сити"):
		return dushanbeProcessFile(file, conn, path)
	default:
		log.Printf("Неизвестный формат файла: %s", path)
		return nil
	}
}

// Попробуем распарсить известные текстовые форматы даты и времени
func normalizeDateTime(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	formats := []string{
		"02.01.06 15:04",
		"02.01.2006 15:04:05",
		"02.01.2006 15:04",
		"02.01.2006",
		"2006-01-02 15:04:05.000",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}

	for _, layout := range formats {
		if t, err := time.Parse(layout, value); err == nil {
			return t, nil
		}
	}

	// Попытка как Excel-дата (в виде числа, например: "45500.5")
	if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
		excelEpoch := time.Date(1899, 12, 30, 0, 0, 0, 0, time.UTC)
		return excelEpoch.Add(time.Duration(floatVal * 24 * float64(time.Hour))), nil
	}

	// Попытка как чистое время "349" / "161838"
	if len(value) <= 6 {
		// Преобразуем в HHMMSS (добавим нули спереди)
		for len(value) < 6 {
			value = "0" + value
		}
		if t, err := time.Parse("150405", value); err == nil {
			now := time.Now()
			return time.Date(now.Year(), now.Month(), now.Day(), t.Hour(), t.Minute(), t.Second(), 0, time.Local), nil
		}
	}

	return time.Time{}, fmt.Errorf("неизвестный формат времени: %q", value)
}

// Парсим строку с денежной суммой в Float64
func parseAmount(s string) float64 {
	s = strings.ReplaceAll(s, " ", "")
	s = strings.ReplaceAll(s, ",", ".")
	amount, _ := strconv.ParseFloat(s, 64)
	return amount
}

// Добавляем в таблицу Payments
func insertPayment(conn *pgx.Conn, p Payment) error {
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
