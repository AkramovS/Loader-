package utils

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Парсим строку с денежной суммой в Float64

func ParseAmount(s string) float64 {
	s = strings.ReplaceAll(s, " ", "")
	s = strings.ReplaceAll(s, ",", ".")
	amount, _ := strconv.ParseFloat(s, 64)
	return amount
}

// Попробуем распарсить известные текстовые форматы даты и времени
func NormalizeDateTime(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
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
		return excelEpoch.Add(time.Duration(floatVal * 24 * float64(time.Hour))), nil
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

	return time.Time{}, fmt.Errorf("неизвестный формат времени: %q", value)
}
