package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/xuri/excelize/v2"
	"log"
	"path/filepath"
	"regexp"
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
	// Подключение к PostgreSQL
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
		fmt.Printf("📄 Обработка файла: %s\n", file)
		if err := processFile(file, conn); err != nil {
			log.Printf("❌ Ошибка в файле %s: %v", file, err)
		}
	}

	fmt.Println("✅ Загрузка завершена.")
}

func processFile(path string, conn *pgx.Conn) error {
	f, err := excelize.OpenFile(path)
	if err != nil {
		return fmt.Errorf("Не удалось открыть файл: %w", err)
	}
	defer f.Close()

	isAlif := strings.Contains(path, "Алиф")
	isZudamal := strings.Contains(path, "Зудамал")

	if isAlif {
		return alifProccesFile(f, conn, path)
	}
	if isZudamal {
		return errors.New("Not implemented yet")
	}
	//

	return nil
}

func cleanAccount(raw string) string {
	// Убираем всё, что после точки, плюса, пробела и т.д.
	re := regexp.MustCompile(`^\d+`)
	return re.FindString(raw)
}

func normalizeDateTime(value string) (time.Time, error) {
	value = strings.TrimSpace(value)

	// Попробуем распарсить известные текстовые форматы даты и времени
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
		t := excelEpoch.Add(time.Duration(floatVal * 24 * float64(time.Hour)))
		return t, nil
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

	// Не удалось распознать
	return time.Time{}, fmt.Errorf("неизвестный формат времени: %q", value)
}

func parseAnyDateTime(value string) time.Time {
	value = strings.TrimSpace(value)

	// Попытка как текстовая дата
	formats := []string{
		"02.01.06 15:04",
		"02.01.2006 15:04:05",
		"2006-01-02 15:04:05.000",
		"02.01.2006 15:04",
		"02.01.2006",
	}
	for _, layout := range formats {
		if t, err := time.Parse(layout, value); err == nil {
			return t
		}
	}

	// Попытка распарсить как Excel-дата-число
	if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
		// Excel-даты начинаются с 1899-12-30
		excelEpoch := time.Date(1899, 12, 30, 0, 0, 0, 0, time.UTC)
		d := excelEpoch.Add(time.Duration(floatVal * 24 * float64(time.Hour)))
		return d
	}

	log.Printf("⚠️ Неизвестный формат даты: %q — подставляется текущая дата", value)
	return time.Now()
}

func parseAmount(s string) float64 {
	s = strings.ReplaceAll(s, " ", "")
	s = strings.ReplaceAll(s, ",", ".")
	amount, _ := strconv.ParseFloat(s, 64)
	return amount
}

func insertPayment(conn *pgx.Conn, p Payment) error {
	_, err := conn.Exec(context.Background(),
		`INSERT INTO payments (file_name, payment_system, payment_id, amount, account_number, payment_datetime, uploaded_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		p.FileName, p.PaymentSystem, p.PaymentID, p.Amount, p.AccountNumber, p.PaymentDateTime, p.UploadedAt)
	if err != nil {
		log.Println(err)
	}
	return err
}
