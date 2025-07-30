package main

import (
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/xuri/excelize/v2"
	"log"
	"path/filepath"
	"strings"
	"time"
)

func dushanbeProccesFile(f *excelize.File, conn *pgx.Conn, path string) error {
	sheet := f.GetSheetName(0)
	rows, err := f.GetRows(sheet)
	if err != nil {
		return fmt.Errorf("Не удалось прочитать строки: %w", err)
	}

	if len(rows) == 0 {
		return errors.New("Empty file")
	}

	// Автоопределение заголовков
	headers := make(map[string]int)
	for j, cell := range rows[0] {
		lc := strings.TrimSpace(cell)
		switch lc {
		case "транзакции":
			headers["ID"] = j
		case "суммма":
			headers["amount"] = j
		case "Л.С":
			headers["account"] = j
		case "Дата":
			headers["transactionTime"] = j
		case "время":
			headers["transactionTimeHours"] = j
		case "поставщик":
			headers["providerName"] = j
		}
	}

	if len(headers) < 6 {
		return errors.New(fmt.Sprintf("Не хватает столбцов ожидается 6, но получаем %d", len(headers)))
	}

	for i, row := range rows {
		if i == 0 {
			log.Println("Skip 0 row because it is naming row")
			continue
		}
		if len(row) < 3 {
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
			isBabilonOK := strings.Contains(row[idx], "Babilon-T")
			okInternetOk := strings.Contains(row[idx], "Интернет")
			if !isBabilonOK || !okInternetOk {
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

			if idx1, ok := headers["transactionTimeHours"]; ok {
				if idx1 > len(row) {
					log.Printf("Not have column ID, invalid row id is %d", i)
					continue
				}
				if len(row[0]) == 0 {
					log.Printf("Not have column ID, invalid row id is %d", i)
					continue
				}
			}

			PaymentDataTime, err = parseYYMMDDAndTimeString(row[idx], row[idx+1])
			if err != nil {
				log.Println(err.Error())
				continue
			}
		}

		payment := Payment{
			FileName:      filepath.Base(path),
			PaymentSystem: "Dushanbe city",
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

// parseDDMMYYAndTimeString — парсит дату в формате "DDMMYY" и время как строку ("349", "161838", и т.д.)
func parseYYMMDDAndTimeString(dateStr, timeStr string) (time.Time, error) {
	dateStr = strings.TrimSpace(dateStr)
	timeStr = strings.TrimSpace(timeStr)

	// Парсим дату: "250501" → 01.05.2025
	dateVal, err := time.Parse("060102", dateStr)
	if err != nil {
		return time.Time{}, fmt.Errorf("не удалось распарсить дату: %v", err)
	}

	// Нормализуем время: "349" → "000349", "161838" остаётся
	for len(timeStr) < 6 {
		timeStr = "0" + timeStr
	}

	t, err := time.Parse("150405", timeStr)
	if err != nil {
		return time.Time{}, fmt.Errorf("не удалось распарсить время: %v", err)
	}

	// Комбинируем дату и время
	result := time.Date(dateVal.Year(), dateVal.Month(), dateVal.Day(), t.Hour(), t.Minute(), t.Second(), 0, time.Local)
	return result, nil
}
