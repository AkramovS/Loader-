package usecases

import (
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/xuri/excelize/v2"
	"log"
	"path/filepath"
	"strings"
)

func PaymentsProcessor(conn *pgx.Conn) {
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
		return zudamalProcessFile(file, conn, path)
	case strings.Contains(strings.ToLower(path), "международн"):
		return ibtProcessFile(file, conn, path)
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
