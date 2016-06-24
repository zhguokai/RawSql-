package dhwdb

import "database/sql"

//数据库客户端结构
type dbClient struct {
	connDB *sql.DB
}

type NestObjMap map[string]ObjMap

func (p NestObjMap)GetMapValue(mapKey, key string, defaultValue interface{}) (interface{}){
	if obj, ok := p[mapKey]; ok {
		return obj.Get(key, defaultValue)
	}
	return defaultValue
}

//
type ObjMap map[string]interface{}

func (p ObjMap)Get(key string, defaultValue interface{}) (interface{}) {
	if value, ok := p[key]; ok {
		return value
	} else {
		return defaultValue
	}
}
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

type OrmSql string
//批量参数模型
type OrmParams []interface{}