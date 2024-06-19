package mongo

import (
	"context"
	"skillfactory/31a.3.1/pkg/storage"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Хранилище данных.
type Store struct {
	db *mongo.Client
	mu sync.Mutex
}

// Конструктор объекта хранилища.
func New(constr string) (*Store, error) {
	db, err := mongo.Connect(context.Background(), options.Client().ApplyURI(constr))
	if err != nil {
		return nil, err
	}
	s := Store{
		db: db,
		mu: sync.Mutex{},
	}
	return &s, nil
}

// Непубличный метод для получения мапы авторов публикаций.
// Данный метод работает в паре с методом "Posts".
func (s *Store) authors() (map[int]string, error) {
	// работаем с базой данных "posts" и коллекцией "authors".
	collection := s.db.Database("posts").Collection("authors")

	// ищем все записи в коллекции.
	cursor, err := collection.Find(context.Background(), bson.D{{}})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())

	// подготавливаем слайс авторов.
	authors := make(map[int]string, 0)

	// проходим по полученным записям из коллекции.
	for cursor.Next(context.Background()) {
		var a storage.Author

		// десериализуем запись в экземпляр структуры "storage.Author".
		if err := cursor.Decode(&a); err != nil {
			return nil, err
		}

		// добавление имени автора в мапу результатов
		authors[a.ID] = a.Name
	}

	// Проверяем на последнюю ошибку курсора поиска в коллекции.
	if err := cursor.Err(); err != nil {
		return nil, err
	}

	// возвращаем мапу полученных авторов.
	return authors, nil
}

// Posts получает все публикации.
func (s *Store) Posts() ([]storage.Post, error) {
	// получаем мапу авторов публикаций.
	authors, err := s.authors()
	if err != nil {
		return nil, err
	}

	// работаем с базой данных "posts" и коллекцией "posts".
	collection := s.db.Database("posts").Collection("posts")

	// ищем все записи в коллекции.
	cursor, err := collection.Find(context.Background(), bson.D{{}})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())

	// подготавливаем слайс публикаций.
	var posts []storage.Post

	// проходим по полученным записям из коллекции.
	for cursor.Next(context.Background()) {
		var p storage.Post

		// десериализуем запись в экземпляр структуры "storage.Post".
		if err := cursor.Decode(&p); err != nil {
			return nil, err
		}

		// указываем имя автора публикации полученное методом "authors".
		p.AuthorName = authors[p.AuthorID]

		// добавление переменной в массив результатов
		posts = append(posts, p)
	}

	// Проверяем на последнюю ошибку курсора поиска в коллекции.
	if err := cursor.Err(); err != nil {
		return nil, err
	}

	// возвращаем слайс полученных публикаций.
	return posts, nil
}

// AddPost добавляет публикацию.
func (s *Store) AddPost(p storage.Post) error {
	// работаем с базой данных "posts" и коллекцией "posts".
	collection := s.db.Database("posts").Collection("posts")

	// блокируем новые запросы на создание публикации.
	s.mu.Lock()
	defer s.mu.Unlock()

	var last storage.Post

	// для соблюдения контракта "storage.Post" в котором ID нужно
	// заполнять числовым идентификатором, получаем последнюю запись в коллекции
	// и испольщуем её ID + 1 в операции создания новой записи.
	collection.FindOne(context.Background(), bson.D{{}}, &options.FindOneOptions{
		Sort: bson.M{
			"_id": -1,
		},
	}).Decode(&last)

	// создаем новую запись публикации с учетом предыдущей записи.
	_, err := collection.InsertOne(context.Background(), bson.M{
		"_id":          last.ID + 1,
		"author_id":    p.AuthorID,
		"title":        p.Title,
		"content":      p.Content,
		"created_at":   time.Now().Unix(),
		"published_at": time.Now().Unix(),
	})

	return err
}

// UpdatePost обновляет публикацию.
func (s *Store) UpdatePost(p storage.Post) error {
	// работаем с базой данных "posts" и коллекцией "posts".
	collection := s.db.Database("posts").Collection("posts")

	// обновляем запись в коллекции по ID публикации.
	_, err := collection.UpdateOne(context.Background(), bson.D{primitive.E{Key: "_id", Value: p.ID}}, bson.D{{
		Key: "$set", Value: bson.D{
			primitive.E{Key: "author_id", Value: p.AuthorID},
			primitive.E{Key: "title", Value: p.Title},
			primitive.E{Key: "content", Value: p.Content},
			primitive.E{Key: "created_at", Value: p.CreatedAt},
			primitive.E{Key: "published_at", Value: p.PublishedAt},
		},
	}})

	return err
}

// DeletePost удаляет публикацию.
func (s *Store) DeletePost(p storage.Post) error {
	// работаем с базой данных "posts" и коллекцией "posts".
	collection := s.db.Database("posts").Collection("posts")

	// удаляем запись из коллекции по ID публикации.
	_, err := collection.DeleteOne(context.Background(), bson.D{primitive.E{
		Key: "_id", Value: p.ID,
	}})

	return err
}
