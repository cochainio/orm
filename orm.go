package orm

import (
	"strings"
	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mssql"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	"github.com/rs/xid"

	"github.com/cochainio/orm/bulk_insert"
)

var Singleton *DB

type IDModel struct {
	ID string `gorm:"primary_key;size:20"`
}

type TimeModel struct {
	CreatedAt time.Time `gorm:"index"`
	UpdatedAt time.Time `gorm:"index"`
}

type Model struct {
	ID        string    `gorm:"primary_key;size:20"`
	CreatedAt time.Time `gorm:"index"`
	UpdatedAt time.Time `gorm:"index"`
}

func Instantiate(dsn string, enableLog bool) {
	if Singleton != nil {
		panic("orm has been instantiated")
	}

	args := strings.Split(dsn, "://")
	db, err := gorm.Open(args[0], args[1])
	if err != nil {
		panic(err.Error())
	}

	db.SingularTable(true)
	db.LogMode(enableLog)

	gorm.AddNamingStrategy(&gorm.NamingStrategy{
		DB: func(name string) string {
			return name
		},
		Table: func(name string) string {
			return name
		},
		Column: func(name string) string {
			return name
		},
	})

	beforeCreateCallback := func(scope *gorm.Scope) {
		if !strings.HasSuffix(scope.TableName(), "deleted") {
			pf := scope.PrimaryField()
			if pf != nil && (pf.Name == "ID" || pf.DBName == "ID") && pf.IsBlank {
				scope.SetColumn("ID", xid.New().String())
			}
		} else {
			if scope.HasColumn("At") {
				scope.SetColumn("At", gorm.NowFunc())
			}
		}
	}

	db.Callback().Create().Before("gorm:before_create").Register("before_create_callback", beforeCreateCallback)

	Singleton = &DB{
		DB: db,
	}
}

type DB struct {
	*gorm.DB
}

func (db *DB) BulkCreate(objects interface{}, opts ...bulk_insert.BuilderOpt) error {
	return bulk_insert.NewBuilder(opts...).Exec(db.DB, objects)
}

type TX struct {
	*gorm.DB
	committed bool
}

func (db *DB) Begin() *TX {
	return &TX{
		DB: db.DB.Begin(),
	}
}

func (tx *TX) End() {
	if !tx.committed {
		tx.Rollback()
	}
}

func (tx *TX) Commit(noPanic ...bool) error {
	tx.DB.Commit()

	if tx.DB.Error != nil {
		if len(noPanic) > 0 && noPanic[0] {
			return tx.DB.Error
		}
		panic(tx.DB.Error.Error())
	}

	tx.committed = true
	return nil
}

func (tx *TX) BulkCreate(objects interface{}, opts ...bulk_insert.BuilderOpt) error {
	return bulk_insert.NewBuilder(opts...).Exec(tx.DB, objects)
}

func IsRecordNotFound(err error) bool {
	if errors, ok := err.(gorm.Errors); ok {
		for _, err := range errors {
			if err == gorm.ErrRecordNotFound {
				return true
			}
		}
	}
	return err == gorm.ErrRecordNotFound
}
