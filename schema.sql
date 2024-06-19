DROP TABLE IF EXISTS posts, authors;

CREATE TABLE authors (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE posts (
    id SERIAL PRIMARY KEY,
    author_id INTEGER REFERENCES authors(id) NOT NULL,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    published_at BIGINT NOT NULL
);

INSERT INTO authors (id, name) VALUES (0, 'Skill Factory');
INSERT INTO posts (id, author_id, title, content, created_at, published_at) VALUES (0, 0, 'The Go Memory Model', 'The Go memory model specifies the conditions under which reads of a variable in one goroutine can be guaranteed to observe values produced by writes to the same variable in a different goroutine.', 0, 0);