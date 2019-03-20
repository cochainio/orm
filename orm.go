package orm

import (
	"strings"

	"github.com/jinzhu/gorm"
	"github.com/rs/xid"
)

var Singleton *DB

func Instantiate(dsn string) {
	if Singleton != nil {
		panic("orm has been instantiated")
	}

	args := strings.Split(dsn, "://")
	db, err := gorm.Open(args[0], args[1])
	if err != nil {
		panic(err.Error())
	}

	db.LogMode(true)

	beforeCreateCallback := func(scope *gorm.Scope) {
		if !strings.HasSuffix(scope.TableName(), "deleted") {
			if scope.HasColumn("ID") {
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
