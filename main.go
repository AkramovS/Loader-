package main

import (
	"Loader/config"
	"Loader/db"
	"Loader/usecases"
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
	"log"
)

func main() {
	// Подключение к PostgresSQL
	_ = godotenv.Load()
	cfg := config.LoadConfigFromEnv()

	conn, err := pgx.Connect(context.Background(), cfg.DSN())
	if err != nil {
		log.Fatalf("Ошибка подключения к базе данных:", err)
	}
	defer conn.Close(context.Background())
	log.Println("Успешное подключение к базе данных")

	if err := db.CreateTable(conn); err != nil {
		log.Fatalf("Не получилось создать таблиицу: %v", err)
	}
	log.Println("Таблица либо существует, либо успешно создана")

	usecases.PaymentsProcessor(conn)
}
