//Copyright (c) 2015 Erfan Akbarimanesh
//The source code is completely free, you can customize for you'r self
package goodb

import (
    "database/sql"
  _ "github.com/go-sql-driver/mysql"
    "fmt"
    "errors"
    "strings"
    "strconv"
    "sort"
)

type (
    MainModel struct {
        DB          *sql.DB
        table       string
        parameter   string
        condition   string
        sorting     string
        limited     string
    }
    
    Config struct {
        Hostname    string
        Port        string
        DBName      string
        MSUsername  string
        MSPassword  string
        Charset     string
    }
    
    rowsType  map[uint64]map[string]interface{}
)

var (
    ErrConnectionNotFound  = errors.New("Database connection not found !")
    ErrValueNotFound       = errors.New("Value not found !")
)

const (
    DBMTYPE  = "mysql"
)

func (mdb *MainModel) SetupGOODB() (*MainModel, error){
    conf := Config{
        Hostname   : "localhost",
        Port       : "2080",
        DBName     : "mydb",
        MSUsername : "root",
        MSPassword : "1234",
        Charset    : "utf8",
    }
    dataSourceName := fmt.Sprintf("%s:%s@/%s?charset=%s", conf.MSUsername, conf.MSPassword, conf.DBName, conf.Charset)
    db, err := sql.Open(DBMTYPE, dataSourceName)
    CheckErr(err)
    
    if err := db.Ping(); err != nil {
        return mdb, err
    }
    mdb.DB = db
    return mdb, err
}

func (mdb *MainModel) Table(tblnm string) *MainModel{
    mdb.table = tblnm
    return mdb
}

func (mdb *MainModel) Where(cparameter string) *MainModel {
    mdb.condition = fmt.Sprintf(" WHERE %v", cparameter)
    return mdb
}

func (mdb *MainModel) SortBy(classify, kind string) *MainModel {
    mdb.sorting = fmt.Sprintf(" ORDER BY `%s` %s", classify, kind)
    return mdb
}

//Limited([2]int{0, -1})  >>> 0
//Limited([2]int{0, 10})  >>> 0-10
func (mdb *MainModel) Limited(lparam [2]int32) *MainModel {
    var starter uint32 = uint32(lparam[0])
    var ls string 
    if lparam[1] == -1 {
        ls = fmt.Sprintf(" LIMIT %d", starter)
    } else {
        ls = fmt.Sprintf(" LIMIT %d,%d", starter, lparam[1])
    }
    mdb.limited = ls
    return mdb
}

func (mdb *MainModel) RowCount(parameter interface{}) (uint64, error) {
    var count uint64 = 0
    var condition string
    switch parameter.(type) {
        case string :
            condition = "WHERE " + parameter.(string)
        case nil :
            condition = ""
    }
    sqlCommand := fmt.Sprintf("SELECT COUNT(*) FROM `%s` %v", mdb.table, condition)
    err := mdb.DB.QueryRow(sqlCommand).Scan(&count)
    if err != nil {
        return 0, err
    }
    return count, err 
}

func (mdb *MainModel) Select(fields string) *sql.Rows {   
    sqlCommand := fmt.Sprintf("SELECT %s FROM %s %v", fields, mdb.table, mdb.condition)
    res, err := mdb.DB.Query(sqlCommand)
    CheckErr(err)
    return res
}

func (mdb *MainModel) RemoveByParam(parameter string) (uint64, error) {
    if mdb.DB == nil {
        fmt.Printf("")
    }
    sqlCommand  := fmt.Sprintf("DELETE FROM `%s` WHERE %s", mdb.table, parameter)
    result, err := mdb.DB.Exec(sqlCommand)
    CheckErr(err)
    affect, err := result.RowsAffected()
    CheckErr(err)
    if affect == 0 {
        return 0, ErrValueNotFound
    }
    return uint64(affect), err
}

func (mdb *MainModel) InsertToDB(data map[string]interface{}) (uint64, error) {
    if mdb.DB == nil {
        return 0,  ErrConnectionNotFound
    }
    var fields, values []string
    
    for field, value := range data {
        fields = append(fields, field)
        switch value.(type) {
            case int8, int16, int, int32, int64 :
                values = append(values, strconv.Itoa(value.(int)))  
            case float32, float64 :
                values = append(values, strconv.FormatFloat(value.(float64), 'G', -1, 64))
            case string :
                values = append(values, value.(string))
        }
    }
        
    fl := fmt.Sprintf("`%v`", strings.Join(fields, "`,`"))
    vl := fmt.Sprintf("'%v'", strings.Join(values, "','"))
    sqlCommand  := fmt.Sprintf("INSERT INTO `%s`(%s) VALUES(%s)", mdb.table, fl, vl)
    result, err := mdb.DB.Exec(sqlCommand)
    if err != nil {
        return 0, err
    }
        
    lastId, err := result.LastInsertId()
    if err != nil {
        return 0, err
    }
    return uint64(lastId), err
}

func (mdb *MainModel) UpdateData(data map[string]interface{}) (uint64, error) {
    if mdb.DB == nil {
        return 0, ErrConnectionNotFound
    }
    var newVal []string
    var formatedStr string
    for field, value := range data {
        switch value.(type) {
            case int8, int16, int, int32, int64 :
                formatedStr = fmt.Sprintf("%v = %v", field, value.(int64))
            case float32, float64 :
                formatedStr = fmt.Sprintf("%v = %v", field, strconv.FormatFloat(value.(float64), 'G', -1, 64))
            case string :
                formatedStr = fmt.Sprintf("%v = '%s'", field, value.(string))
        }
        newVal = append(newVal, formatedStr)
    }
    sqlCommand := fmt.Sprintf("UPDATE %s SET %v %v", mdb.table, strings.Join(newVal, ","), mdb.condition)
    result, err := mdb.DB.Exec(sqlCommand)
    if err != nil {
        return 0, err
    }
    affect, err := result.RowsAffected()
    if err != nil {
        return 0, err
    }
    
    return uint64(affect), err
}

func RowsResult(rs *sql.Rows) rowsType {
    cols, count := rowsColumn(rs)
    
    var (
        sqlResult  rowsType       =  make(rowsType)
        crude      []sql.RawBytes =  make([]sql.RawBytes, count)
        storeAddr  []interface{}  =  make([]interface{}, len(crude))
        counter    uint64 = 0
    )
        
    for keyNum := range crude {
		storeAddr[keyNum] = &crude[keyNum]
	}
        
    for rs.Next() {
        sqlResult[counter] = make(map[string]interface{})
        CheckErr(rs.Scan(storeAddr...))
        
        for key, val := range crude {
            sqlResult[counter][cols[key]] = string(val)
        }
        counter++
    }

    return sqlResult
}

func FetchResults(input rowsType) []map[string]interface{} {
    var keys []int
    for k, _ := range input {
        keys = append(keys, int(k))
    }
    sort.Ints(keys)
    
    var m []map[string]interface{}
    
    for _, k := range keys {
        m = append(m, input[uint64(k)])
    }
    return m
}

func rowsColumn(rs *sql.Rows)  ([]string, int64) {
    cols, err := rs.Columns()
    CheckErr(err)
    return cols, int64(len(cols))
}

func (mdb *MainModel) Reset() {
    mdb.condition = ""
    mdb.limited   = ""
    mdb.sorting   = ""
    mdb.parameter = ""
}

func (mdb *MainModel) CloseDB() {
    mdb.DB.Close()
}

func CheckErr(err error) {
    if err != nil {
        fmt.Printf("Error: %s\r\n", err)
    }
}
