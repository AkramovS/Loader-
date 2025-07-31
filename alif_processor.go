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

// Считывание строк файла Алиф
func alifProcessFile(f *excelize.File, conn *pgx.Conn, path string) error {
	sheet := f.GetSheetName(0)
	rows, err := f.GetRows(sheet)
	if err != nil {
		return fmt.Errorf("не удалось прочитать строки: %w", err)
	}

	if len(rows) == 0 {
		return errors.New("empty file")
	}

	// Автоопределение заголовков
	headers := make(map[string]int)
	for j, cell := range rows[0] {
		lc := strings.TrimSpace(cell)
		switch lc {
		case "ID":
			headers["ID"] = j
		case "Сумма провайдера":
			headers["amount"] = j
		case "Счёт":
			headers["account"] = j
		case "Дата оплаты":
			headers["transactionTime"] = j
		case "Название провайдера":
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
		if len(row) < 3 {
			log.Printf("Пропущена неполная строка %d: %v", i+2, row)
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
			PaymentDataTime, err = normalizeDateTime(row[idx])
			if err != nil {
				log.Println(err.Error())
				continue
			}
		}

		payment := Payment{
			FileName:      filepath.Base(path),
			PaymentSystem: "Alif",
			PaymentID:     paymentID,

			Amount:          amount,
			AccountNumber:   acountnumber,
			PaymentDateTime: PaymentDataTime,

			UploadedAt: time.Now(),
		}

		if err := insertPayment(conn, payment); err != nil {
			log.Printf("Ошибка вставки в БД (строка %d): %v", i+2, err)
		}
	}

	return nil
}
