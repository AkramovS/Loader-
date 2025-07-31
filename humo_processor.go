package main

import (
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/xuri/excelize/v2"
	"log"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// Считывание строк файла Хумо
func humoProcessFile(f *excelize.File, conn *pgx.Conn, path string) error {
	sheet := f.GetSheetName(0)
	rows, err := f.GetRows(sheet)
	if err != nil {
		return fmt.Errorf("Не удалось прочитать строки: %w", err)
	}

	if len(rows) < 6 {
		return errors.New("Empty file")
	}

	// Автоопределение заголовков
	headers := make(map[string]int)
	for j, cell := range rows[5] {
		lc := strings.TrimSpace(cell)
		switch lc {
		case "ID":
			headers["ID"] = j
		case "Amount":
			headers["amount"] = j
		case "Account":
			headers["account"] = j
		case "CreatedAt":
			headers["transactionTime"] = j
		case "ServiceDesc":
			headers["providerName"] = j
		}
	}

	if len(headers) < 5 {
		return errors.New(fmt.Sprintf("Не хватает столбцов ожидается 5, но получаем %d", len(headers)))
	}

	for i, row := range rows {
		if i == 0 {
			log.Println("Skip 0 row because it is naming row")
			continue
		}
		if len(row) < 5 {
			log.Printf("⚠️ Пропущена неполная строка %d: %v", i+2, row)
			continue
		}

		paymentID := ""
		if idx, ok := headers["ID"]; ok {
			if idx > len(row) {
				log.Printf("Not have column ID, invalid row id is %d", i)
				continue
			}
			if len(row[0]) == 0 {
				log.Printf("Not have column ID, invalid row id is %d", i)
				continue
			}

			paymentID = row[idx]
		}

		if idx, ok := headers["providerName"]; ok {
			if row[idx] != "Babilon-T Internet" {
				log.Println("Skip row, wait Babilon-T Internet id ", paymentID)
				continue
			}
		} else {
			continue
		}

		amount := 0.0
		if idx, ok := headers["amount"]; ok {
			if idx > len(row) {
				log.Printf("Not have column ID, invalid row id is %d", i)
				continue
			}
			if len(row[0]) == 0 {
				log.Printf("Not have column ID, invalid row id is %d", i)
				continue
			}
			amount = parseAmount(row[idx])
		}

		acountnumber := ""
		if idx, ok := headers["account"]; ok {
			if idx > len(row) {
				log.Printf("Not have column ID, invalid row id is %d", i)
				continue
			}
			if len(row[0]) == 0 {
				log.Printf("Not have column ID, invalid row id is %d", i)
				continue
			}
			acountnumber = row[idx]
		}

		var PaymentDataTime time.Time
		if idx, ok := headers["transactionTime"]; ok {
			if idx > len(row) {
				log.Printf("Not have column ID, invalid row id is %d", i)
				continue
			}
			if len(row[0]) == 0 {
				log.Printf("Not have column ID, invalid row id is %d", i)
				continue
			}
			PaymentDataTime, err = extractAndParseDateTime(row[idx])
			if err != nil {
				log.Println(err.Error())
				continue
			}
		}

		payment := Payment{
			FileName:      filepath.Base(path),
			PaymentSystem: "Humo",
			PaymentID:     paymentID,

			Amount:          amount,
			AccountNumber:   acountnumber,
			PaymentDateTime: PaymentDataTime,

			UploadedAt: time.Now(),
		}

		if err := insertPayment(conn, payment); err != nil {
			log.Printf("❌ Ошибка вставки в БД (строка %d): %v", i+2, err)
		}
	}

	return nil
}

func extractAndParseDateTime(s string) (time.Time, error) {
	// Удаляем лишние пробелы
	s = strings.TrimSpace(s)

	// Ищем подстроку вида 4-2-2 (дата)
	reDate := regexp.MustCompile(`\d{4}-\d{2}-\d{2}`)
	datePart := reDate.FindString(s)

	// Ищем подстроку вида 2:2:2 (время)
	reTime := regexp.MustCompile(`\d{2}:\d{2}:\d{2}`)
	timePart := reTime.FindString(s)

	if datePart == "" || timePart == "" {
		return time.Time{}, fmt.Errorf("не удалось найти корректную дату или время в строке: %q", s)
	}

	// Собираем строку и парсим
	combined := datePart + " " + timePart
	layout := "2006-01-02 15:04:05"

	t, err := time.Parse(layout, combined)
	if err != nil {
		return time.Time{}, fmt.Errorf("не удалось распарсить как дату-время: %v", err)
	}
	return t, nil
}
