package mysql

import (
	"database/sql"
	"errors"
	"reflect"
	"strings"
)

type DBer interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	Exec(query string, args ...interface{}) (sql.Result, error)
}

type myErrCode int

const (
	NoErr myErrCode = iota
	// 单行查询的时候没有一行数据
	NoOnceRowsDate
	// 需要传slice
	NeedSlice
	// 遍历使用的函数错误
	SliceFuncErr
)

type iMyDbError interface {
	error
	Code() myErrCode
	IsThisCode(code myErrCode) bool
}
type myDbError struct {
	err  error
	code myErrCode
}

func (m *myDbError) Error() string {
	if m != nil && m.err != nil {
		return m.err.Error()
	}
	return ""
}

func (m *myDbError) Code() myErrCode {
	if m != nil {
		return m.code
	}
	return NoErr
}

func (m *myDbError) IsThisCode(code myErrCode) bool {
	return m != nil && m.code == code
}

func IsThisCode(err error, code myErrCode) bool {
	if err, ok := err.(iMyDbError); ok {
		return err.IsThisCode(code)
	}
	return false
}

// DbQuery 查询数据库表
func DbQuery(q DBer, ty interface{}, sql string, args ...interface{}) error {
	rows, err := q.Query(sql, args...)
	if err != nil {
		return err
	}
	return rowsModel(rows, ty)
}

// DbQueryWithIndexFunc 对查询的每一条结果执行f,err!=nil空时返回err
func DbQueryWithIndexFunc(q DBer, slice interface{}, f func(sliceIndex int) error, sql string, args ...interface{}) error {
	rows, err := q.Query(sql, args...)
	if err != nil {
		return err
	}
	return rowsModelWithFunc(rows, slice, f)
}

// DbQueryWithModelFunc 对查询的每一条结果执行f,err!=nil空时返回err
func DbQueryWithModelFunc(q DBer, model interface{}, f func(model interface{}) error, sql string, args ...interface{}) error {
	rows, err := q.Query(sql, args...)
	if err != nil {
		return err
	}
	return rowsModelWithModelFunc(rows, model, f)
}

func rowsModelWithModelFunc(rows *sql.Rows, model interface{}, f func(model interface{}) error) error {
	defer rows.Close()
	t := reflect.TypeOf(model)
	kind := t.Kind()
	if kind == reflect.Slice || reflect.Ptr == kind {
		return errors.New("ty不能是slice和地址")
	}
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	adder := getAddressIndex(t, columns)
	for rows.Next() {
		dest := reflect.New(t)
		adders := getRowIndex(dest, adder)
		err = rows.Scan(adders...)
		if err != nil {
			return err
		}
		if err := f(dest.Elem().Interface()); err != nil {
			return &myDbError{code: SliceFuncErr, err: err}
		}
	}
	return nil
}

func rowsModelWithFunc(rows *sql.Rows, slice interface{}, f func(sliceIndex int) error) error {
	defer rows.Close()
	v := reflect.ValueOf(slice)
	t := reflect.TypeOf(slice)
	if t.Kind() != reflect.Ptr {
		return errors.New("ty必须传地址")
	}
	for t.Kind() == reflect.Ptr {
		if !v.Elem().CanSet() {
			v.Set(reflect.New(t.Elem()))
		}
		v = v.Elem()
		t = t.Elem()
	}
	if t.Kind() != reflect.Slice {
		return &myDbError{code: NeedSlice}
	}
	dt := t.Elem()
	for dt.Kind() == reflect.Ptr {
		dt = dt.Elem()
	}
	sl := reflect.MakeSlice(t, 0, 0)
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	adder := getAddressIndex(dt, columns)
	kind := t.Elem().Kind()
	index := 0
	for rows.Next() {
		dest := reflect.New(dt)
		adders := getRowIndex(dest, adder)
		err = rows.Scan(adders...)
		if err != nil {
			return err
		}
		//区分切片元素是否指针
		switch kind {
		case reflect.Ptr:
			sl = reflect.Append(sl, dest)
		default:
			sl = reflect.Append(sl, dest.Elem())
		}
		v.Set(sl)
		if err := f(index); err != nil {
			return &myDbError{code: SliceFuncErr, err: err}
		}
		index++
	}
	return nil
}

func rowsModel(rows *sql.Rows, args ...interface{}) error {
	defer rows.Close()
	if len(args) == 1 {
		ty := args[0]
		v := reflect.ValueOf(ty)
		t := reflect.TypeOf(ty)
		if t.Kind() != reflect.Ptr {
			return errors.New("ty必须传地址")
		}
		for t.Kind() == reflect.Ptr {
			if !v.Elem().CanSet() {
				v.Set(reflect.New(t.Elem()))
			}
			v = v.Elem()
			t = t.Elem()
		}
		switch t.Kind() {
		case reflect.Slice:
			dt := t.Elem()
			for dt.Kind() == reflect.Ptr {
				dt = dt.Elem()
			}
			sl := reflect.MakeSlice(t, 0, 0)
			columns, err := rows.Columns()
			if err != nil {
				return err
			}
			adder := getAddressIndex(dt, columns)
			kind := t.Elem().Kind()
			for rows.Next() {
				dest := reflect.New(dt)
				adders := getRowIndex(dest, adder)
				err = rows.Scan(adders...)
				if err != nil {
					return err
				}
				//区分切片元素是否指针
				switch kind {
				case reflect.Ptr:
					sl = reflect.Append(sl, dest)
				default:
					sl = reflect.Append(sl, dest.Elem())
				}
			}
			if v.CanSet() {
				v.Set(sl)
			}
		default:
			columns, err := rows.Columns()
			if err != nil {
				return err
			}
			adder := getAddressIndex(t, columns)
			count := 0
			for rows.Next() {
				count++
				dest := reflect.New(t)
				adders := getRowIndex(dest, adder)
				err = rows.Scan(adders...)
				if err != nil {
					return err
				}
				if v.CanSet() {
					v.Set(dest.Elem())
				}
				break
			}
			if count == 0 {
				return &myDbError{code: NoOnceRowsDate, err: errors.New("没有一行数据")}
			}
		}
		return nil
	} else {
		for rows.Next() {
			var list []interface{}
			for _, d := range args {
				if _, ok := d.(sql.Scanner); !ok {
					d = &NullData{
						data: d,
					}
				}
				list = append(list, d)
			}
			return rows.Scan(list...)
		}
		return &myDbError{code: NoOnceRowsDate, err: errors.New("没有一行数据")}
	}
}

// 获取查询数据列对应实体的索引
func getAddressIndex(dt reflect.Type, columns []string) [][]int {
	adders := make([][]int, len(columns))
	mapIndex := getFieldList(dt)
	for i, v1 := range columns {
		if v, ok := mapIndex[v1]; ok {
			adders[i] = v
		} else {
			adders[i] = nil
		}
	}
	return adders
}

// 获取数据库列对应的实体地址
func getRowIndex(dest reflect.Value, index [][]int) []interface{} {
	adders := make([]interface{}, len(index))
	dest = dest.Elem()
	if dest.Kind() != reflect.Struct {
		d := dest.Addr().Interface()
		if _, ok := d.(sql.Scanner); !ok {
			d = &NullData{
				data: d,
			}
		}
		adders[0] = d
		for i := 1; i < len(index); i++ {
			d := &NullData{
				data: new(interface{}),
			}
			adders[i] = d
		}
		return adders
	}
	for i := 0; i < len(index); i++ {
		son := index[i]
		destSon := dest
		if len(son) == 0 {
			d := &NullData{
				data: new(interface{}),
			}
			adders[i] = d
			continue
		}
		for j := 0; j < len(son); j++ {
			destSon = destSon.Field(son[j])
			if destSon.Kind() == reflect.Ptr {
				if destSon.IsNil() {
					nVf := reflect.New(destSon.Type().Elem())
					destSon.Set(nVf)
				}
				destSon = destSon.Elem()
			}
		}
		d := destSon.Addr().Interface()
		if _, ok := d.(sql.Scanner); !ok {
			d = &NullData{
				data: d,
			}
		}
		adders[i] = d
	}
	return adders
}
func getFieldList(src reflect.Type) (mapIndex map[string][]int) {
	mapIndex = map[string][]int{}
	var f func(reflect.Type, []int)
	f = func(dest reflect.Type, l []int) {
		for dest.Kind() == reflect.Ptr {
			dest = dest.Elem()
		}
		switch dest.Kind() {
		case reflect.Struct:
			for n := 0; n < dest.NumField(); n++ {
				tf := dest.Field(n)
				if tf.Type.Kind() == reflect.Ptr {
					if tf.Type.Elem().Kind() == reflect.Struct && tf.Anonymous {
						l = append(l, n)
						f(tf.Type.Elem(), l)
						l = l[:len(l)-1]
						continue
					}
				}
				//如果字段值是time类型之外的struct，递归取址
				if tf.Type.Kind() == reflect.Struct && tf.Type.Name() != "Time" {
					if tf.Anonymous {
						l = append(l, n)
						f(tf.Type, l)
						l = l[:len(l)-1]
						continue
					}
				}
				column := strings.Split(tf.Tag.Get("json"), ",")[0]
				if column == "" {
					column = tf.Name
				}
				l = append(l, n)
				mapIndex[column] = append([]int(nil), l...)
				l = l[:len(l)-1]
			}
		}
	}
	f(src, []int{})
	return mapIndex
}
