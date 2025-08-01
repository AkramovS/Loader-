package config

import (
	"fmt"
	"os"
)

type Config struct {
	User     string
	Password string
	Host     string
	Port     string
	DBName   string
	SSLMode  string
}

// LoadConfigFromEnv загружает конфиг из переменных окружения с дефолтами
func LoadConfigFromEnv() *Config {
	return &Config{
		User:     getEnv("PG_USER", "postgres"),
		Password: getEnv("PG_PASSWORD", ""),
		Host:     getEnv("PG_HOST", "localhost"),
		Port:     getEnv("PG_PORT", "5432"),
		DBName:   getEnv("PG_DB", "payments"),
		SSLMode:  getEnv("PG_SSLMODE", "disable"),
	}
}

// DSN возвращает строку подключения к Postgres
func (c *Config) DSN() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=%s",
		c.User, c.Password, c.Host, c.Port, c.DBName, c.SSLMode,
	)
}

// getEnv читает переменную окружения с дефолтом
func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return defaultVal
}
