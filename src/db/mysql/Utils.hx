package db.mysql;

import mysql.MySqlError;

class Utils {
    public static inline var SQL_TABLE_EXISTS = "SELECT * FROM information_schema.TABLES WHERE TABLE_NAME=?;";

    public static function MySqlError2DatabaseError(error:MySqlError, call:String) {
        var dbError = new DatabaseError(error.message, call);
        return dbError;
    }

    public static function buildCreateTable(tableName:String, columns:Array<ColumnDefinition>, typeMapper:IDataTypeMapper) {
        var sql = 'CREATE TABLE ${tableName} (\n';

        var columnParts = [];
        var primaryKey:String = null;
        for (column in columns) {
            var type = typeMapper.haxeTypeToDatabaseType(column.type);
            var columnSql = '    ${column.name}';
            columnSql += ' ${type}';
            var suffix = '';
            if (column.options != null) {
                for (option in column.options) {
                    switch (option) {
                        case PrimaryKey:
                            primaryKey = column.name;
                        case AutoIncrement:        
                            suffix += ' AUTO_INCREMENT';
                        case NotNull:
                            suffix += ' NOT NULL';
                    }
                }
            }
            if (suffix.length > 0) {
                columnSql += suffix;
            }

            columnParts.push(columnSql);
        }

        if (primaryKey != null) {
            columnParts.push('    PRIMARY KEY ($primaryKey)');
        }

        sql += columnParts.join(",\n");

        sql += ');';
        return sql;
    }
}