package db.mysql;

import mysql.MySqlError;
import promises.Promise;
import mysql.DatabaseConnection as MySqlDatabaseConnection;

class Utils {
    public static inline var SQL_TABLE_EXISTS = "SELECT * FROM information_schema.TABLES WHERE TABLE_NAME=?;";
    public static inline var SQL_LIST_TABLES_AND_FIELDS = "SELECT table_name, column_name, ordinal_position FROM information_schema.columns
                                                           WHERE table_schema = 'persons'
                                                           ORDER BY table_name,ordinal_position;";

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

    public static function loadFullDatabaseSchema(connection:MySqlDatabaseConnection):Promise<DatabaseSchema> {
        return new Promise((resolve, reject) -> {
            var schema:DatabaseSchema = {};
            connection.all(SQL_LIST_TABLES_AND_FIELDS).then(result -> {
                for (r in result.data) {
                    var table = schema.findTable(r.table_name);
                    if (table == null) {
                        table = {
                            name: r.table_name
                        };
                        schema.tables.push(table);
                    }
                    table.columns.push({
                        name: r.column_name,
                        type: null
                    });
                }
                resolve(schema);
            }, error -> {
                reject(error);
            });
        });
    }

    public static function buildAddColumns(tableName:String, columns:Array<ColumnDefinition>, typeMapper:IDataTypeMapper):String {
        var sql = 'ALTER TABLE ${tableName}\n';

        for (column in columns) {
            var type = typeMapper.haxeTypeToDatabaseType(column.type);
            sql += 'ADD ${column.name} ${type}';
        }

        sql += ';';

        return sql;
    }

    public static function buildRemoveColumns(tableName:String, columns:Array<ColumnDefinition>, typeMapper:IDataTypeMapper):String {
        var sql = 'ALTER TABLE ${tableName}\n';

        for (column in columns) {
            sql += 'DROP COLUMN ${column.name}';
        }

        sql += ';';

        return sql;
    }
}