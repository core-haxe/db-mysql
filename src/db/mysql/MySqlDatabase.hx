package db.mysql;

import sys.db.Mysql;
import mysql.MySqlError;
import promises.Promise;
import mysql.DatabaseConnection as MySqlDatabaseConnection;
import db.mysql.Utils.*;
import db.utils.SqlUtils.*;

class MySqlDatabase implements IDatabase {
    private var _connection:MySqlDatabaseConnection = null;
    private var _relationshipDefs:RelationshipDefinitions = null;

    public function setProperty(name:String, value:Any):Void {

    }
    public function getProperty(name:String, defaultValue:Any):Any {
        return null;
    }

    public function new() {
    }

    public function config(details:Dynamic) {
        // TODO: validate details
        _connection = new MySqlDatabaseConnection({
            database: details.database,
            host: details.host,
            user: details.user,
            pass: details.pass
        });
    }

    public function schema():Promise<DatabaseResult<DatabaseSchema>> {
        return new Promise((resolve, reject) -> {
            resolve(null);
        });
    }

    public function defineTableRelationship(field1:String, field2:String) {
        if (_relationshipDefs == null) {
            _relationshipDefs = new RelationshipDefinitions();
        }
        _relationshipDefs.add(field1, field2);
    }

    public function definedTableRelationships():RelationshipDefinitions {
        return _relationshipDefs;
    }

    public function connect():Promise<DatabaseResult<Bool>> {
        return new Promise((resolve, reject) -> {
            _connection.open().then(response -> {
                resolve(new DatabaseResult(this, response.data));
            }, (error:MySqlError) -> {
                reject(MySqlError2DatabaseError(error, "connect"));
            });
        });
    }

    public function disconnect():Promise<DatabaseResult<Bool>> {
        return new Promise((resolve, reject) -> {
            _connection.close();
            resolve(new DatabaseResult(this, true));
        });
    }

    public function table(name:String):Promise<DatabaseResult<ITable>> {
        return new Promise((resolve, reject) -> {
            _connection.get(SQL_TABLE_EXISTS, name).then(response -> {
                var table:ITable = new MySqlTable(this);
                table.name = name;
                table.exists = !(response.data == null);
                resolve(new DatabaseResult(this, table));
            }, (error:MySqlError) -> {
                reject(MySqlError2DatabaseError(error, "table"));
            });
        });
    }

    public function createTable(name:String, columns:Array<ColumnDefinition>):Promise<DatabaseResult<ITable>> {
        return new Promise((resolve, reject) -> {
            var sql = buildCreateTable(name, columns, MySqlDataTypeMapper.get());
            _connection.exec(sql).then(response -> {
                var table:ITable = new MySqlTable(this);
                table.name = name;
                table.exists = true;
                resolve(new DatabaseResult(this, table));
            }, (error:MySqlError) -> {
                reject(MySqlError2DatabaseError(error, "createTable"));
            });
        });
    }

    public function deleteTable(name:String):Promise<DatabaseResult<Bool>> {
        return new Promise((resolve, reject) -> {
            reject(new DatabaseError("not implemented", "deleteTable"));
        });
    }
}