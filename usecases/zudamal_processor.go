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
	"regexp"
	"strings"
	"time"
)

const Zudamal = "Zudamal"

// Считывание строк файла Зудамал
func zudamalProcessFile(f *excelize.File, conn *pgx.Conn, path string) error {
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
	for j, cell := range rows[0] {
		switch strings.TrimSpace(cell) {
		case "№ тран":
			headers["ID"] = j
		case "Сумма":
			headers["amount"] = j
		case "Номер":

			headers["account"] = j
		case "Дата операции":
			headers["transactionTime"] = j
		}
	}

	if len(headers) < 4 {
		return errors.New(fmt.Sprintf("Не хватает столбцов ожидается 4, но получаем %d", len(headers)))
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
			PaymentDataTime, err = utils.NormalizeDateTime(row[idx])
			if err != nil {
				log.Println(err.Error())
				continue
			}
		}

		payment := models.Payment{
			FileName:        filepath.Base(path),
			PaymentSystem:   Zudamal,
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

// Убираем всё, что после точки, плюса, пробела и т.д.
func CleanAccount(raw string) string {
	re := regexp.MustCompile(`^\d+`)
	return re.FindString(raw)
}
