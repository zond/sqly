package sqly

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

type DB struct {
	sqlx.DB
}

func (db *DB) Write(ctx context.Context, f func(*Tx) error) error {
	tx, err := db.Beginy(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	if err := f(tx); err != nil {
		if err := tx.Rollback(); err != nil {
			return errors.WithStack(err)
		}
		return errors.WithStack(err)
	}
	if err := tx.Commit(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (db *DB) Read(ctx context.Context, f func(*Tx) error) error {
	tx, err := db.BeginTxy(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return errors.WithStack(err)
	}
	if err := f(tx); err != nil {
		if err := tx.Rollback(); err != nil {
			return errors.WithStack(err)
		}
		return errors.WithStack(err)
	}
	if err := tx.Commit(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (db *DB) Upsert(ctx context.Context, structPointer any, overwrite bool) error {
	return Upsert(ctx, db, structPointer, overwrite)
}

func (db *DB) CreateTableIfNotExists(ctx context.Context, prototype any) error {
	return CreateTableIfNotExists(ctx, db, prototype)
}

func (db *DB) BeginTxy(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := db.BeginTxx(ctx, opts)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &Tx{*tx}, nil
}

func (db *DB) Beginy(ctx context.Context) (*Tx, error) {
	return db.BeginTxy(ctx, nil)
}

type Tx struct {
	sqlx.Tx
}

func (tx *Tx) Upsert(ctx context.Context, structPointer any, overwrite bool) error {
	return Upsert(ctx, tx, structPointer, overwrite)
}

func (tx *Tx) CreateTableIfNotExists(ctx context.Context, prototype any) error {
	return CreateTableIfNotExists(ctx, tx, prototype)
}

type StackTracer interface {
	StackTrace() errors.StackTrace
}

func Open(driverName string, dataSourceName string) (*DB, error) {
	db, err := sqlx.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	db.MapperFunc(func(s string) string { return s })
	return &DB{*db}, nil
}

func Upsert(ctx context.Context, execer sqlx.ExecerContext, structPointer any, overwrite bool) error {
	val := reflect.ValueOf(structPointer)
	if val.Kind() != reflect.Ptr {
		return errors.Errorf("%v is not a reflect.Ptr", structPointer)
	}
	val = val.Elem()
	if val.Kind() != reflect.Struct {
		return errors.Errorf("%v is not a pointer to a reflect.Struct", structPointer)
	}
	typ := val.Type()
	cols := []string{}
	qmarks := []string{}
	params := []any{}
	var primaryKeyFieldToSet *reflect.Value
	for fieldIndex := 0; fieldIndex < typ.NumField(); fieldIndex++ {
		field := typ.Field(fieldIndex)
		skip := false
		if field.IsExported() {
			for _, tag := range strings.Split(field.Tag.Get("sqly"), ",") {
				fieldVal := val.Field(fieldIndex)
				if tag == "pkey" && fieldVal.CanInt() && fieldVal.Int() == 0 {
					primaryKeyFieldToSet = &fieldVal
					skip = true
				}
			}
			if !skip {
				cols = append(cols, fmt.Sprintf("`%s`", field.Name))
				qmarks = append(qmarks, "?")
				params = append(params, val.Field(fieldIndex).Interface())
			}
		}
	}
	replace := ""
	if overwrite {
		replace = "OR REPLACE "
	}
	res, err := execer.ExecContext(ctx, fmt.Sprintf("INSERT %sINTO `%s` (%s) VALUES (%s)", replace, typ.Name(), strings.Join(cols, ","), strings.Join(qmarks, ",")), params...)
	if err != nil {
		return errors.WithStack(err)
	}
	if primaryKeyFieldToSet != nil {
		lastID, err := res.LastInsertId()
		if err != nil {
			return errors.WithStack(err)
		}
		primaryKeyFieldToSet.SetInt(lastID)
	}
	return nil
}

type index struct {
	cols   []string
	unique bool
}

var (
	uniqueWithRegexp = regexp.MustCompile(`uniqueWith\\((.*)\\)`)
	indexWithRegexp  = regexp.MustCompile(`indexWith\\((.*)\\)`)
)

func CreateTableIfNotExists(ctx context.Context, execer sqlx.ExecerContext, prototype any) error {
	val := reflect.ValueOf(prototype)
	if val.Kind() != reflect.Struct {
		return errors.Errorf("%v is not a reflect.Struct", prototype)
	}
	typ := val.Type()
	primaryKeyCol := ""
	primaryKeySQLType := ""
	cols := []string{}
	sqlTypes := []string{}
	indices := []index{}
	for fieldIndex := 0; fieldIndex < typ.NumField(); fieldIndex++ {
		field := typ.Field(fieldIndex)
		if field.IsExported() {
			sqlType := ""
			switch field.Type.Kind() {
			case reflect.String:
				sqlType = "TEXT"
			case reflect.Uint:
				fallthrough
			case reflect.Uint8:
				fallthrough
			case reflect.Uint16:
				fallthrough
			case reflect.Uint32:
				fallthrough
			case reflect.Uint64:
				fallthrough
			case reflect.Int:
				fallthrough
			case reflect.Int8:
				fallthrough
			case reflect.Int16:
				fallthrough
			case reflect.Int32:
				fallthrough
			case reflect.Int64:
				sqlType = "INTEGER"
			case reflect.Float32:
				sqlType = "REAL"
			case reflect.Float64:
				sqlType = "REAL"
			case reflect.Bool:
				sqlType = "INTEGER"
			default:
				return errors.Errorf("%v isn't of a supported type", field)
			}
			isPkey := false
			for _, tag := range strings.Split(field.Tag.Get("sqly"), ",") {
				switch tag {
				case "unique":
					indices = append(indices, index{
						cols:   []string{field.Name},
						unique: true,
					})
				case "index":
					indices = append(indices, index{
						cols:   []string{field.Name},
						unique: false,
					})
				case "pkey":
					isPkey = true
					primaryKeyCol = field.Name
					primaryKeySQLType = sqlType
				default:
					if match := uniqueWithRegexp.FindStringSubmatch(tag); match != nil {
						indices = append(indices, index{
							cols:   append([]string{field.Name}, strings.Split(match[1], ";")...),
							unique: true,
						})
					} else if match = indexWithRegexp.FindStringSubmatch(tag); match != nil {
						indices = append(indices, index{
							cols:   append([]string{field.Name}, strings.Split(match[1], ";")...),
							unique: false,
						})
					}
				}
				if !isPkey {
					cols = append(cols, field.Name)
					sqlTypes = append(sqlTypes, sqlType)
				}
			}
		}
	}
	if primaryKeyCol == "" {
		return errors.Errorf("%v doesn't have a PRIMARY KEY (field tagged `sqly:\"pkey\"`)", prototype)
	}
	if _, err := execer.ExecContext(ctx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (`%s` %s PRIMARY KEY)", typ.Name(), primaryKeyCol, primaryKeySQLType)); err != nil {
		return errors.WithStack(err)
	}
	for colIndex, col := range cols {
		if _, err := execer.ExecContext(ctx, fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN `%s` %s", typ.Name(), col, sqlTypes[colIndex])); err != nil && !strings.Contains(err.Error(), "duplicate column name") {
			return errors.WithStack(err)
		}
	}
	for _, index := range indices {
		unique := ""
		if index.unique {
			unique = "UNIQUE "
		}
		escapedCols := make([]string, len(index.cols))
		for colIndex, col := range index.cols {
			escapedCols[colIndex] = fmt.Sprintf("`%s`", col)
		}
		if _, err := execer.ExecContext(ctx, fmt.Sprintf("CREATE %sINDEX IF NOT EXISTS `%s.%s` ON `%s` (%s)", unique, typ.Name(), strings.Join(index.cols, ","), typ.Name(), strings.Join(escapedCols, ","))); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}
