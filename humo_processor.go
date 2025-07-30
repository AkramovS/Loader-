package main

import (
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

func humoProccesFile(f *excelize.File, conn *pgx.Conn, path string) error {
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

func cleanInvalidDateTime(raw string) (time.Time, error) {
	// Ищем YYYY-MM-DD и HH:MM:SS
	re := regexp.MustCompile(`(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})`)
	matches := re.FindStringSubmatch(raw)

	if len(matches) != 7 {
		return time.Time{}, fmt.Errorf("❌ не удалось извлечь дату и время: %s", raw)
	}

	year, _ := strconv.Atoi(matches[1])
	month, _ := strconv.Atoi(matches[2])
	day, _ := strconv.Atoi(matches[3])
	hour, _ := strconv.Atoi(matches[4])
	minute, _ := strconv.Atoi(matches[5])
	second, _ := strconv.Atoi(matches[6])

	// Ограничим значения до допустимых
	if month > 12 {
		month = 12
	}
	if day > 31 {
		day = 31
	}
	if hour > 23 {
		hour = 23
	}
	if minute > 59 {
		minute = 59
	}
	if second > 59 {
		second = 59
	}

	// Собираем дату
	return time.Date(year, time.Month(month), day, hour, minute, second, 0, time.UTC), nil
}
