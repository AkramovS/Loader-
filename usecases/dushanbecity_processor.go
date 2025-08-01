package usecases

import (
	"Loader/db"
	"Loader/models"
	"Loader/utils"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/xuri/excelize/v2"
	"log"
	"path/filepath"
	"strings"
	"time"
)

const DushanbeCity = "Dushanbe city"

// Считывание строк файла Душанбе Сити
func dushanbeProcessFile(f *excelize.File, conn *pgx.Conn, path string) error {
	sheet := f.GetSheetName(0)
	rows, err := f.GetRows(sheet)
	if err != nil {
		return fmt.Errorf("Не удалось прочитать строки: %w", err)
	}

	if len(rows) == 0 {
		return errors.New("Пустой файл")
	}

	// Автоопределение заголовков
	headers := make(map[string]int)
	for j, cell := range rows[0] {
		switch strings.TrimSpace(cell) {
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
		return errors.New(fmt.Sprintf("Не хватает столбцов: найдено %d", len(headers)))
	}

	for i, row := range rows {
		if i == 0 {
			log.Println("Пропущена первая строка (заголовки)")
			continue
		}
		if len(row) < 3 {
			log.Printf("Пропущена неполная строка %d: %v", i+2, row)
			continue
		}

		paymentID := ""
		if idx, ok := headers["ID"]; ok {
			if idx > len(row) {
				log.Printf("Не удалось найти столбец  — ошибка в строке %d", i)
				continue
			}
			if len(row[0]) == 0 {
				log.Printf("Не удалось найти столбец  — ошибка в строке %d", i)
				continue
			}

			paymentID = row[idx]
		}

		if idx, ok := headers["providerName"]; ok {
			if !strings.Contains(row[idx], "Babilon-T") || !strings.Contains(row[idx], "Интернет") {
				log.Println("Пропущена строка,ожидается Babilon-T Internet id ", paymentID)
				continue
			}
		} else {
			continue
		}

		amount := 0.0
		if idx, ok := headers["amount"]; ok {
			if idx > len(row) {
				log.Printf("Не удалось найти столбец  — ошибка в строке %d", i)
				continue
			}
			if len(row[0]) == 0 {
				log.Printf("Не удалось найти столбец  — ошибка в строке %d", i)
				continue
			}
			amount = utils.ParseAmount(row[idx])
		}

		acountnumber := ""
		if idx, ok := headers["account"]; ok {
			if idx > len(row) {
				log.Printf("Не удалось найти столбец  — ошибка в строке %d", i)
				continue
			}
			if len(row[0]) == 0 {
				log.Printf("Не удалось найти столбец  — ошибка в строке %d", i)
				continue
			}
			acountnumber = CleanAccount(row[idx])
		}

		var PaymentDataTime time.Time
		if idx, ok := headers["transactionTime"]; ok {
			if idx > len(row) {
				log.Printf("Не удалось найти столбец  — ошибка в строке %d", i)
				continue
			}
			if len(row[0]) == 0 {
				log.Printf("Не удалось найти столбец  — ошибка в строке %d", i)
				continue
			}

			if idx1, ok := headers["transactionTimeHours"]; ok {
				if idx1 > len(row) {
					log.Printf("Не удалось найти столбец  — ошибка в строке %d", i)
					continue
				}
				if len(row[0]) == 0 {
					log.Printf("Не удалось найти столбец  — ошибка в строке %d", i)
					continue
				}
			}

			PaymentDataTime, err = parseYYMMDDAndTimeString(row[idx], row[idx+1])
			if err != nil {
				log.Println(err.Error())
				continue
			}
		}

		payment := models.Payment{
			FileName:        filepath.Base(path),
			PaymentSystem:   DushanbeCity,
			PaymentID:       paymentID,
			Amount:          amount,
			AccountNumber:   acountnumber,
			PaymentDateTime: PaymentDataTime,
			UploadedAt:      time.Now(),
		}

		if err := db.InsertPayment(conn, payment); err != nil {
			log.Printf("Ошибка вставки в БД (строка %d): %v", i+2, err)
		}
	}

	return nil
}

// Функция parseDDMMYYAndTimeString — парсит дату в формате "DDMMYY" и время как строку ("349", "161838", и т.д.)
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
