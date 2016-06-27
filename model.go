package sqlutil

import "database/sql"

//数据库连接URL
const MysqlDriverURL = "%s:%s@tcp(%s:%s)/%s?timeout=90s"

//数据库客户端结构
type dbClient struct {
	connDB *sql.DB
}

//返回多条记录
type RowMaps []RowMap

//查询结果
type RowMap map[string]interface{}

func (p RowMap)Get(key string, defaultValue interface{}) (interface{}) {
	if value, ok := p[key]; ok {
		return value
	} else {
		return defaultValue
	}
}

//ORM对象集合
type OrmObjList []OrmObj

//ORM对象
type OrmObj struct {
	//sql语句
	Sql   string
	//Sql参数
	Param OrmParams
}

//SQL语句
type OrmSql string

//批量参数模型
type OrmParams []interface{}