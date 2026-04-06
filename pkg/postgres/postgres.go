package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/DRSN-tech/go-backend/internal/cfg"
	"github.com/DRSN-tech/go-backend/pkg/e"
	"github.com/DRSN-tech/go-backend/pkg/logger"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// PgDatabase инкапсулирует подключение к PostgreSQL и управление миграциями.
type PgDatabase struct {
	Pool *pgxpool.Pool // TODO: проверить надо ли делать это поле публичным
	Dsn  string
	cfg  *cfg.PGDBCfg
}

func NewPgDatabase(pool *pgxpool.Pool, cfg *cfg.PGDBCfg, dsn string) *PgDatabase {
	return &PgDatabase{Pool: pool, cfg: cfg, Dsn: dsn}
}

// Connect устанавливает соединение с PostgreSQL.
func Connect(cfg *cfg.PGDBCfg) (*PgDatabase, error) {
	const op = "PgDatabase.Connect"
	dsn := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		cfg.Host,
		cfg.Port,
		cfg.User,
		cfg.Password,
		cfg.DBName,
		cfg.SSLMode,
	)

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		return nil, e.Wrap(op, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	if err := pool.Ping(ctx); err != nil {
		return nil, e.Wrap(op, err)
	}

	return NewPgDatabase(pool, cfg, dsn), nil
}

func (db *PgDatabase) Ping() error {
	const op = "PgDatabase.Ping"
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	if err := db.Pool.Ping(ctx); err != nil {
		return e.Wrap(op, err)
	}

	return nil
}

// Close корректно закрывает пул соединений к базе данных.
func (db *PgDatabase) Close() {
	if db.Pool != nil {
		db.Pool.Close()
	}
}

// RunMigrations применяет ожидающие миграции из директории db/migrations.
func (db *PgDatabase) RunMigrations(logger logger.Logger) error {
	const (
		op                 = "PgDatabase.RunMigrations"
		driverName         = "pgx"
		databaseDriverName = "postgres"
		sourceURL          = "file://db/migrations"
	)

	sqlDb, err := sql.Open(driverName, db.Dsn)
	if err != nil {
		return err
	}
	defer func() { _ = sqlDb.Close() }()

	driver, err := postgres.WithInstance(sqlDb, &postgres.Config{})
	if err != nil {
		return err
	}

	m, err := migrate.NewWithDatabaseInstance(
		sourceURL,
		databaseDriverName,
		driver,
	)
	if err != nil {
		return e.Wrap(op, err)
	}

	err = m.Up()
	if err != nil {
		if errors.Is(err, migrate.ErrNoChange) {
			return nil
		}
		return e.Wrap(op, err)
	}

	logger.Infof("migrations applied successfully")
	return nil
}
