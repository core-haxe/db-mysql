package db.mysql;

import mysql.MySqlError;

class Utils {
    public static inline var SQL_TABLE_EXISTS = "SELECT * FROM information_schema.TABLES WHERE TABLE_NAME=?;";

    public static function MySqlError2DatabaseError(error:MySqlError, call:String) {
        var dbError = new DatabaseError(error.message, call);
        return dbError;
    }

    public static function buildCreateDatabase(databaseName:String):String {
        return 'CREATE DATABASE IF NOT EXISTS ${databaseName};';
    }

    public static function buildDropDatabase(databaseName:String):String {
        return 'DROP DATABASE IF EXISTS ${databaseName};';
    }

    public static function buildSelectDatabase(databaseName:String):String {
        return 'USE ${databaseName};';
    }

    public static function buildHasDatabase(databaseName:String):String {
        return 'SHOW DATABASES LIKE \'${databaseName}\';';
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