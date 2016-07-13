package sqlutil

//sqlutil 封装使用sql语句对Mysql的操作
//基于go-sql-driver/mysql
//使用方式
//	package main
//
//	func func main() {
//		sqlutil.RegisterDB(driver, url string, maxIdle int, maxOpen int)
//	}
//
//
//


import (
	_ "github.com/go-sql-driver/mysql"
	"database/sql"
	"errors"
	"log"
	"strconv"
)

var dbPool map[string]*dBTool = map[string]*dBTool{}

//数据库句柄
type dBTool struct {
	conn *sql.DB
}

func UseDB(key string) *dBTool {
	if db, ok := dbPool[key]; ok {
		return db
	} else {
		return nil
	}
}

// RegisterDB 注册数据库驱动
func AddDB(key, driver, url string, maxIdle int, maxOpen int) (err error) {
	if db, err := sql.Open(driver, url); err == nil {
		db.SetMaxIdleConns(maxIdle)
		db.SetMaxOpenConns(maxOpen)
		err := db.Ping()
		if err != nil {
			return err
		}
		dbItem := &dBTool{}
		dbItem.conn = db
		dbPool[key] = dbItem
		return nil
	} else {
		return err
	}
}

//通过SQL语句查询单条记录，无参数,多条记录时返回首条记录
func (p *dBTool)QueryRow(sqlStr string) (row RowMap, err error) {
	//判断SQL语句是否为空
	if "" == sqlStr {
		return nil, errors.New("传入的SQL语句不能为空！")
	}
	//调用go-sql-server\Mysql驱动查询
	rows, err := p.conn.Query(sqlStr)
	if err != nil {
		log.Println("mysql query error", err.Error())
		return nil, err
	}
	//延时关闭Rows
	defer rows.Close()
	//获取记录列
	if columns, err := rows.Columns(); err != nil {
		return nil, err
	} else {
		//拼接记录Map
		values := make([]sql.RawBytes, len(columns))
		scans := make([]interface{}, len(columns))

		for i := range values {
			scans[i] = &values[i]
		}

		for rows.Next() {

			_ = rows.Scan(scans...)
			each := make(RowMap)

			for i, col := range values {
				each[columns[i]] = string(col)
			}

			row = each
			//仅读取第一条记录
			break

		}
		return row, nil
	}

}

//根据SQL语句与参数列表查询单条记录,SQL语句中参数为?占位符
//
func (p *dBTool)QueryRowWithParam(sqlStr string, params OrmParams) (singleMap RowMap, err error) {
	//判断SQL语句是否为空
	if "" == sqlStr {
		return nil, errors.New("传入的SQL语句不能为空！")
	}
	if nil == params {
		return nil, errors.New("传入的参数不能为空！")
	}
	singleMap = RowMap{}
	//调用go-sql-server\Mysql驱动查询
	rows, err := p.conn.Query(sqlStr, params...)
	if err != nil {
		log.Println("mysql query error", err.Error())
		return nil, err
	}
	//延时关闭Rows
	defer rows.Close()
	//获取记录列
	if columns, err := rows.Columns(); err != nil {
		return nil, err
	} else {
		//拼接记录Map
		values := make([]sql.RawBytes, len(columns))
		scans := make([]interface{}, len(columns))

		for i := range values {
			scans[i] = &values[i]
		}

		for rows.Next() {

			_ = rows.Scan(scans...)
			each := make(RowMap)

			for i, col := range values {
				each[columns[i]] = string(col)
			}

			singleMap = each
			//仅读取第一条记录
			break

		}
		return singleMap, nil
	}
}

//通过SQL语句查询多条记录
func (p *dBTool)QueryRows(sqlStr string) (rowMaps RowMaps, err error) {

	//判断SQL语句是否为空
	if "" == sqlStr {
		return nil, errors.New("传入的SQL语句不能为空！")
	}
	//调用go-sql-server\Mysql驱动查询
	rows, err := p.conn.Query(sqlStr)
	if err != nil {
		log.Println("mysql query error", err.Error())
		return nil, err
	}
	rowMaps = RowMaps{}
	//延时关闭Rows
	defer rows.Close()
	//获取记录列
	if columns, err := rows.Columns(); err != nil {
		return nil, err
	} else {
		//拼接记录Map
		values := make([]sql.RawBytes, len(columns))
		scans := make([]interface{}, len(columns))

		for i := range values {
			scans[i] = &values[i]
		}

		for rows.Next() {
			_ = rows.Scan(scans...)
			each := make(map[string]interface{})

			for i, col := range values {
				each[columns[i]] = string(col)
			}

			rowMaps = append(rowMaps, each)

		}
		return rowMaps, nil
	}

}

//通过SQL语句查询多条记录带参数
func (p *dBTool)QueryRowsWithParams(sqlStr string, params OrmParams) (rowMaps RowMaps, err error) {
	//判断SQL语句是否为空
	if "" == sqlStr {
		return nil, errors.New("传入的SQL语句不能为空！")
	}
	if nil == params {
		return nil, errors.New("传入的参数不能为空！")
	}
	rowMaps = RowMaps{}
	//调用go-sql-server\Mysql驱动查询
	rows, err := p.conn.Query(sqlStr, params...)
	if err != nil {
		log.Println("mysql query error", err.Error())
		return nil, err
	}
	//延时关闭Rows
	defer rows.Close()
	//获取记录列
	if columns, err := rows.Columns(); err != nil {
		return nil, err
	} else {
		//拼接记录Map
		values := make([]sql.RawBytes, len(columns))
		scans := make([]interface{}, len(columns))

		for i := range values {
			scans[i] = &values[i]
		}

		for rows.Next() {
			_ = rows.Scan(scans...)
			each := make(map[string]interface{})

			for i, col := range values {
				each[columns[i]] = string(col)
			}

			rowMaps = append(rowMaps, each)

		}
		return rowMaps, nil
	}
}

//执行SQL语句,包含增删改查
func (p *dBTool)executeSqlWithParams(sqlStr string, params OrmParams) (rowCount int64, err error) {
	//开启事务
	tx, err := p.conn.Begin()
	if err != nil {
		log.Println("打开事务:" + err.Error())
		return -1, err
	}
	stmt, err := tx.Prepare(sqlStr)
	if err != nil {
		log.Println("预编译SQL:" + sqlStr + ";" + err.Error())
		return -1, err
	}
	defer stmt.Close()

	res, err := stmt.Exec(params...)
	if err != nil {
		tx.Rollback()
		log.Println("执行语句出错:" + sqlStr + ";" + err.Error())
		return -1, err
	} else {

		rowCount, err := res.RowsAffected()
		if err == nil {
			cerr := tx.Commit()
			if cerr != nil {
				log.Println("提交事务失败:" + cerr.Error())
				return -1, cerr
			}
			return rowCount, nil
		} else {
			tx.Rollback()
			log.Println("查询记录行数:" + err.Error())
			return -1, nil
		}

	}
}

//执行SQL语句,包含增删改查
func (p *dBTool)executeSql(sqlStr string) (rowCount int64, err error) {
	//开启事务
	tx, err := p.conn.Begin()
	if err != nil {
		log.Println("打开事务:" + err.Error())
		return -1, err
	}
	stmt, err := tx.Prepare(sqlStr)
	if err != nil {
		log.Println("预编译SQL:" + sqlStr + ";" + err.Error())
		return -1, err
	}
	defer stmt.Close()

	res, err := stmt.Exec()
	if err != nil {
		tx.Rollback()
		log.Println("执行语句出错:" + sqlStr + ";" + err.Error())
		return -1, err
	} else {

		rowCount, err := res.RowsAffected()
		if err == nil {
			cerr := tx.Commit()
			if cerr != nil {
				log.Println("提交事务失败:" + cerr.Error())
				return -1, cerr
			}
			return rowCount, nil
		} else {
			tx.Rollback()
			log.Println("查询记录行数:" + err.Error())
			return -1, nil
		}

	}
}


//执行单条带参数SQl语句，例如新增、修改、删除开启事务
func (p *dBTool)InsertRowWithParam(sqlStr string, params OrmParams) (rowCount int64, err error) {
	//增加记录
	return p.executeSqlWithParams(sqlStr, params)
}

//更新记录Row，带参数
func (p *dBTool)UpdateRowWithParam(sqlStr string, params OrmParams) (rowCount int64, err error) {
	return p.executeSqlWithParams(sqlStr, params)

}

//删除记录Row，带参数
func (p *dBTool)DeleteRowsWithParam(sqlStr string, params OrmParams) (rowCount int64, err error) {
	return p.executeSqlWithParams(sqlStr, params)
}
//删除记录Row，带参数
func (p *dBTool)DeleteRows(sqlStr string) (rowCount int64, err error) {
	return p.executeSql(sqlStr)
}

//批量执行SQl语句
func (p *dBTool)ExecBatchSqlWithParams(sqlStrs []string, params []OrmParams) (rowCount int64, err error) {
	//判断SQL参数记录数与参数是否一一对应
	if sqlStrs == nil || len(sqlStrs) == 0 {
		return 0, errors.New("sql语句数组不能为空")
	}
	if params == nil || len(params) == 0 {
		return 0, errors.New("语句参数数组不能为空")
	}

	if len(sqlStrs) != len(params) {
		return 0, errors.New("要执行的SQL语句条数与参数数量不一致")
	}

	//开启事务支持
	tx, err := p.conn.Begin()
	if err != nil {
		return 0, err
	} else {
		rowCount = 0
		for i, sql := range sqlStrs {
			res, err := tx.Exec(sql, params[i]...)
			if err != nil {
				//执行过程出错回滚事务
				tx.Rollback()
				return 0, err
				break
			} else {
				rowAffectedCount, err := res.RowsAffected()
				if err != nil {
					tx.Rollback()
					return 0, err
					break
				}
				rowCount = rowCount + rowAffectedCount
			}
		}
		tx.Commit()
		return rowCount, nil
	}

}

//批量执行SQl语句
func (p *dBTool)BatchExecuteWithModel(models OrmObjList) (rowCount int64, err error) {
	if nil == models || len(models) == 0 {
		return 0, errors.New("缺少要执行的SQL语句")
	}
	//开启事务支持
	tx, err := p.conn.Begin()
	if err != nil {
		log.Println("连接数据库失败:" + err.Error())
		return 0, err
	} else {
		rowCount = 0
		for _, mod := range models {
			res, err := tx.Exec(mod.Sql, mod.Param...)
			if err != nil {
				//执行过程出错回滚事务
				tx.Rollback()
				log.Println("执行SQL出错:" + mod.Sql + "," + err.Error())
				return 0, err
				break
			} else {
				rowAffectedCount, err := res.RowsAffected()
				if err != nil {
					tx.Rollback()
					return 0, err
					break
				}
				rowCount = rowCount + rowAffectedCount
			}
		}
		err := tx.Commit()
		if err == nil {

			return rowCount, nil
		} else {
			log.Println("提交事务失败:" + err.Error())
			return 0, err
		}

	}

}

func (p *dBTool)CountRow(sql string, column string, params OrmParams) (rowCount int64, err error) {
	if params == nil || len(params) == 0 {
		row, err := p.QueryRow(sql)
		if err != nil {
			return 0, err
		}
		rowCount := row.Get(column, "0").(string)
		count, _ := strconv.Atoi(rowCount)
		return int64(count), nil
	} else {
		row, err := p.QueryRowWithParam(sql, params)
		if err != nil {
			return 0, err
		}
		rowCount := row.Get(column, "0").(string)
		count, _ := strconv.Atoi(rowCount)
		return int64(count), nil
	}
}
