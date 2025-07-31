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

// Считывание строк файла Межднародный Банк
func IbtProcessFile(f *excelize.File, conn *pgx.Conn, path string) error {
	sheet := f.GetSheetName(0)
	rows, err := f.GetRows(sheet)
	if err != nil {
		return fmt.Errorf("Не удалось прочитать строки: %w ", err)
	}

	if len(rows) == 0 {
		return errors.New("Пустой файл ")
	}

	// Автоопределение заголовков
	headers := make(map[string]int)
	for j, cell := range rows[1] {
		switch strings.TrimSpace(cell) {
		case "ИД Платежа":
			headers["ID"] = j
		case "На счет":
			headers["amount"] = j
		case "Номер":
			headers["account"] = j
		case "Дата":
			headers["transactionTime"] = j
		case "Услуга":
			headers["providerName"] = j
		}
	}

	if len(headers) < 5 {
		return errors.New(fmt.Sprintf("Не хватает столбцов ожидается 5, но получаем %d", len(headers)))
	}

	for i, row := range rows {
		if i == 1 {
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
			if len(row[1]) == 1 {
				log.Printf("Не удалось найти столбец  — ошибка в строке %d", i)
				continue
			}

			paymentID = row[idx]
		}

		if idx, ok := headers["providerName"]; ok {
			if row[idx] != "Babilon-T" {
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
			amount = parseAmount(row[idx])
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
			acountnumber = row[idx]
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
			PaymentDataTime, err = normalizeDateTime(row[idx])
			if err != nil {
				log.Println(err.Error())
				continue
			}
		}

		payment := Payment{
			FileName:        filepath.Base(path),
			PaymentSystem:   "IBT",
			PaymentID:       paymentID,
			Amount:          amount,
			AccountNumber:   acountnumber,
			PaymentDateTime: PaymentDataTime,

			UploadedAt: time.Now(),
		}

		if err := insertPayment(conn, payment); err != nil {
			log.Printf(" Ошибка вставки в БД (строка %d): %v", i+2, err)
		}
	}

	return nil
}
