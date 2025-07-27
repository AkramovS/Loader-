package main

import (
	"context"
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
	AccountNumber   string
	PaymentDateTime time.Time
	Amount          float64
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
		return fmt.Errorf("не удалось открыть файл: %w", err)
	}
	defer f.Close()

	sheet := f.GetSheetName(0)
	rows, err := f.GetRows(sheet)
	if err != nil {
		return fmt.Errorf("Не удалось прочитать строки: %w", err)
	}

	// Автоопределение заголовков
	headers := make(map[string]int)
	for i, row := range rows {
		for j, cell := range row {
			lc := strings.ToLower(strings.TrimSpace(cell))
			switch {
			case strings.Contains(lc, "услуга"):
				headers["service"] = j
			case strings.Contains(lc, "счет"):
				headers["account"] = j
			case strings.Contains(lc, "дата"):
				headers["date"] = j
			case strings.Contains(lc, "время"):
				headers["time"] = j
			case strings.Contains(lc, "сумма"):
				headers["amount"] = j
			}
		}
		if len(headers) >= 3 {
			rows = rows[i+1:]
			break
		}
	}

	for i, row := range rows {
		if len(row) < 3 {
			log.Printf("⚠️ Пропущена неполная строка %d: %v", i+2, row)
			continue
		}

		// Фильтрация по услуге
		if idx, ok := headers["service"]; ok {
			if idx >= len(row) || !strings.Contains(strings.ToLower(row[idx]), "интернет") {
				continue
			}
		}

		account := ""
		if idx, ok := headers["account"]; ok && idx < len(row) {
			account = cleanAccount(row[idx])
		}

		var dt time.Time
		switch {
		case headers["date"] < len(row) && headers["time"] < len(row):
			dt = parseSplitDateTime(row[headers["date"]], row[headers["time"]])
		case headers["date"] < len(row):
			dt = parseAnyDateTime(row[headers["date"]])
		}

		var amount float64
		if idx, ok := headers["amount"]; ok && idx < len(row) {
			amount = parseAmount(row[idx])
		}

		payment := Payment{
			AccountNumber:   account,
			PaymentDateTime: dt,
			Amount:          amount,
		}

		if err := insertPayment(conn, payment); err != nil {
			log.Printf("❌ Ошибка вставки в БД (строка %d): %v", i+2, err)
		}
	}

	return nil
}

func cleanAccount(raw string) string {
	// Убираем всё, что после точки, плюса, пробела и т.д.
	re := regexp.MustCompile(`^\d+`)
	return re.FindString(raw)
}

func parseSplitDateTime(dateStr, timeStr string) time.Time {
	dateVal, err := time.Parse("060102", dateStr)
	if err != nil {
		dateVal = time.Now()
	}
	timeStr = strings.TrimSpace(timeStr)

	// Обработка времени в виде числа, например: 349 -> 00:03:49
	for len(timeStr) < 6 {
		timeStr = "0" + timeStr
	}
	t, err := time.Parse("150405", timeStr)
	if err != nil {
		return dateVal
	}
	return time.Date(dateVal.Year(), dateVal.Month(), dateVal.Day(), t.Hour(), t.Minute(), t.Second(), 0, time.Local)
}

func parseAnyDateTime(value string) time.Time {
	formats := []string{
		"02.01.06 15:04",
		"02.01.2006 15:04:05",
		"2006-01-02 15:04:05.000",
		"02.01.2006 15:04",
		"02.01.2006",
	}
	value = strings.TrimSpace(value)
	for _, layout := range formats {
		if t, err := time.Parse(layout, value); err == nil {
			return t
		}
	}
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
		`INSERT INTO payments (account_number, payment_datetime, amount)
		 VALUES ($1, $2, $3)`,
		p.AccountNumber, p.PaymentDateTime, p.Amount)
	return err
}

/*package main

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
*/
