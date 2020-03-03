package fixtures

import (
	"fmt"
	"github.com/Masterminds/squirrel"
	"github.com/applike/gosoline/pkg/cfg"
	"github.com/applike/gosoline/pkg/db"
	"github.com/applike/gosoline/pkg/mon"
	"reflect"
)

type PlainMySqlFixtureValue []interface{}

type PlainMySqlFixture struct {
	TableName string
	Columns   []string
	Values    []PlainMySqlFixtureValue
}

type plainMySqlFixtureWriter struct {
	logger   mon.Logger
	dbClient db.Client
}

func PlainMySqlFixtureWriterFactory() FixtureWriterFactory {
	return func(config cfg.Config, logger mon.Logger) FixtureWriter {
		dbClient := db.NewClient(config, logger)
		return NewPlainMysqlFixtureWriterWithInterfaces(logger, dbClient)
	}
}

func NewPlainMysqlFixtureWriterWithInterfaces(logger mon.Logger, dbClient db.Client) FixtureWriter {
	return &plainMySqlFixtureWriter{
		logger:   logger,
		dbClient: dbClient,
	}
}

func (m *plainMySqlFixtureWriter) Write(fs *FixtureSet) error {
	for _, item := range fs.Fixtures {
		fixture, ok := item.(*PlainMySqlFixture)

		if !ok {
			return fmt.Errorf("invalid fixture type: %s", reflect.TypeOf(item))
		}

		sql, args, err := buildSql(fixture)

		if err != nil {
			return err
		}

		res, err := m.dbClient.Exec(sql, args...)

		if err != nil {
			return err
		}

		ar, err := res.RowsAffected()

		if err != nil {
			return err
		}

		m.logger.Info(fmt.Sprintf("affected rows while fixture loading: %d", ar))
	}

	m.logger.Infof("loaded %d plain mysql fixtures", len(fs.Fixtures))

	return nil
}

func buildSql(fixture *PlainMySqlFixture) (string, []interface{}, error) {
	insertBuilder := squirrel.Replace(fixture.TableName).PlaceholderFormat(squirrel.Question).Columns(fixture.Columns...)

	for _, values := range fixture.Values {
		insertBuilder = insertBuilder.Values(values...)
	}

	return insertBuilder.ToSql()
}
