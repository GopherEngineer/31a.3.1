package postgres

import (
	"context"
	"skillfactory/31a.3.1/pkg/storage"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Хранилище данных.
type Store struct {
	db *pgxpool.Pool
}

// Конструктор объекта хранилища.
func New(constr string) (*Store, error) {
	db, err := pgxpool.New(context.Background(), constr)
	if err != nil {
		return nil, err
	}
	s := Store{
		db: db,
	}
	return &s, nil
}

func (s *Store) Posts() ([]storage.Post, error) {
	rows, err := s.db.Query(context.Background(), `
		SELECT
			p.id, p.title, p.content, p.author_id, a.name, p.created_at, p.published_at
		FROM
			posts p
			LEFT JOIN authors a ON p.author_id = a.id
		ORDER BY
			id ASC;
	`)
	if err != nil {
		return nil, err
	}
	var posts []storage.Post

	// итерирование по результату выполнения запроса
	// и сканирование каждой строки в переменную
	for rows.Next() {
		var p storage.Post
		err = rows.Scan(
			&p.ID,
			&p.Title,
			&p.Content,
			&p.AuthorID,
			&p.AuthorName,
			&p.CreatedAt,
			&p.PublishedAt,
		)
		if err != nil {
			return nil, err
		}

		// добавление переменной в массив результатов
		posts = append(posts, p)
	}

	// ВАЖНО не забыть проверить rows.Err()
	return posts, rows.Err()
}

func (s *Store) AddPost(p storage.Post) error {
	rows, err := s.db.Query(context.Background(), `
		INSERT INTO posts (author_id, title, content, created_at, published_at)
		VALUES ($1, $2, $3, $4, $5);
	`,
		p.AuthorID,
		p.Title,
		p.Content,
		time.Now().Unix(),
		time.Now().Unix(),
	)
	rows.Close()

	return err
}

// UpdatePost обновляет публикацию по ID.
func (s *Store) UpdatePost(p storage.Post) error {
	rows, err := s.db.Query(context.Background(), `
		UPDATE posts
		SET (author_id, title, content, created_at, published_at) = ($2, $3, $4, $5, $6)
		WHERE id = $1;
	`,
		p.ID,
		p.AuthorID,
		p.Title,
		p.Content,
		p.CreatedAt,
		p.PublishedAt,
	)
	rows.Close()

	return err
}

// DeletePost удаляет публикацию по ID.
func (s *Store) DeletePost(p storage.Post) error {
	rows, err := s.db.Query(context.Background(), `
		DELETE FROM posts
		WHERE id = $1;
	`,
		p.ID,
	)
	rows.Close()

	return err
}
