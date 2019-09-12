package bulk_insert

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/jinzhu/gorm"
)

type Builder struct {
	chunkSize      int
	replace        bool
	excludeColumns []string
}

type BuilderOpt func(*Builder)

func ChunkSizeOpt(chunkSize int) BuilderOpt {
	return func(c *Builder) {
		c.chunkSize = chunkSize
	}
}

func ReplaceOpt(replace bool) BuilderOpt {
	return func(c *Builder) {
		c.replace = replace
	}
}

func ExcludeColumnsOpt(excludeColumns []string) BuilderOpt {
	return func(c *Builder) {
		c.excludeColumns = excludeColumns
	}
}

func NewBuilder(opts ...BuilderOpt) *Builder {
	b := &Builder{
		chunkSize: 2000,
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

func (b *Builder) Exec(db *gorm.DB, objects interface{}) error {
	return BulkInsert(db, objects, b.chunkSize, b.replace, b.excludeColumns...)
}

// Insert multiple records at once
// [objects]        Must be a slice of struct
// [chunkSize]      Number of records to insert at once.
//                  Embedding a large number of variables at once will raise an error beyond the limit of prepared statement.
//                  Larger size will normally lead the better performance, but 2000 to 3000 is reasonable.
// [excludeColumns] Columns you want to exclude from insert. You can omit if there is no column you want to exclude.
func BulkInsert(db *gorm.DB, objects interface{}, chunkSize int, replace bool, excludeColumns ...string) error {
	value := reflect.ValueOf(objects)
	if value.Kind() != reflect.Slice {
		return errors.New("objects must be a slice")
	}
	objectInterfaces := make([]interface{}, value.Len())
	for i := 0; i < value.Len(); i++ {
		objectInterfaces[i] = value.Index(i).Interface()
	}

	// Split records with specified size not to exceed Database parameter limit
	for _, objSet := range splitObjects(objectInterfaces, chunkSize) {
		if err := insertObjSet(db, objSet, replace, excludeColumns...); err != nil {
			return err
		}
	}
	return nil
}

func insertObjSet(db *gorm.DB, objects []interface{}, replace bool, excludeColumns ...string) error {
	if len(objects) == 0 {
		return nil
	}

	firstAttrs, err := extractMapValue(objects[0], excludeColumns)
	if err != nil {
		return err
	}

	attrSize := len(firstAttrs)

	// Scope to eventually run SQL
	mainScope := db.NewScope(objects[0])
	// Store placeholders for embedding variables
	placeholders := make([]string, 0, attrSize)

	// Replace with database column name
	dbColumns := make([]string, 0, attrSize)
	for _, key := range sortedKeys(firstAttrs) {
		dbColumns = append(dbColumns, gorm.ToColumnName(key))
	}

	for _, obj := range objects {
		objAttrs, err := extractMapValue(obj, excludeColumns)
		if err != nil {
			return err
		}

		// If object sizes are different, SQL statement loses consistency
		if len(objAttrs) != attrSize {
			return errors.New("attribute sizes are inconsistent")
		}

		scope := db.NewScope(obj)

		// Append variables
		variables := make([]string, 0, attrSize)
		for _, key := range sortedKeys(objAttrs) {
			scope.AddToVars(objAttrs[key])
			variables = append(variables, "?")
		}

		valueQuery := "(" + strings.Join(variables, ", ") + ")"
		placeholders = append(placeholders, valueQuery)

		// Also append variables to mainScope
		mainScope.SQLVars = append(mainScope.SQLVars, scope.SQLVars...)
	}

	operation := "INSERT"
	if replace {
		operation = "REPLACE"
	}

	mainScope.Raw(fmt.Sprintf("%s INTO %s (%s) VALUES %s",
		operation,
		mainScope.QuotedTableName(),
		strings.Join(dbColumns, ", "),
		strings.Join(placeholders, ", "),
	))

	return db.Exec(mainScope.SQL, mainScope.SQLVars...).Error
}

// Obtain columns and values required for insert from interface
func extractMapValue(value interface{}, excludeColumns []string) (map[string]interface{}, error) {
	if reflect.ValueOf(value).Kind() != reflect.Struct {
		return nil, errors.New("value must be kind of Struct")
	}

	var attrs = map[string]interface{}{}

	for _, field := range (&gorm.Scope{Value: value}).Fields() {
		// Exclude relational record because it's not directly contained in database columns
		_, hasForeignKey := field.TagSettingsGet("FOREIGNKEY")

		if !containString(excludeColumns, field.Struct.Name) && field.StructField.Relationship == nil && !hasForeignKey &&
			!field.IsIgnored && !(field.DBName == "id" && field.IsPrimaryKey) {
			if field.Struct.Name == "CreatedAt" || field.Struct.Name == "UpdatedAt" {
				attrs[field.DBName] = time.Now()
			} else if field.StructField.HasDefaultValue && field.IsBlank {
				// If default value presents and field is empty, assign a default value
				if val, ok := field.TagSettingsGet("DEFAULT"); ok {
					attrs[field.DBName] = val
				} else {
					attrs[field.DBName] = field.Field.Interface()
				}
			} else {
				attrs[field.DBName] = field.Field.Interface()
			}
		}
	}
	return attrs, nil
}
