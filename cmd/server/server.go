package main

import (
	"net/http"

	"skillfactory/31a.3.1/pkg/api"
	"skillfactory/31a.3.1/pkg/storage"
	"skillfactory/31a.3.1/pkg/storage/memdb"
)

// Сервер публикаций.
type server struct {
	db  storage.Interface
	api *api.API
}

func main() {
	// Создаём объект сервера.
	var srv server

	// Создаём объекты баз данных.
	//
	// БД в памяти.
	db := memdb.New()
	/*
		// Реляционная БД PostgreSQL.
		db2, err := postgres.New("postgres://postgres:postgres@server.domain/posts")
		if err != nil {
			log.Fatal(err)
		}
		// Документная БД MongoDB.
		db3, err := mongo.New("mongodb://server.domain:27017/")
		if err != nil {
			log.Fatal(err)
		}
		_, _ = db2, db3
	*/

	// Инициализируем хранилище сервера конкретной БД.
	srv.db = db

	// Создаём объект API и регистрируем обработчики.
	srv.api = api.New(srv.db)

	// Запускаем веб-сервер на порту 8080 на всех интерфейсах.
	// Предаём серверу маршрутизатор запросов,
	// поэтому сервер будет все запросы отправлять на маршрутизатор.
	// Маршрутизатор будет выбирать нужный обработчик.
	http.ListenAndServe(":8080", srv.api.Router())
}
