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
    private var _config:Dynamic;

    private var _properties:Map<String, Any> = [];
    public function setProperty(name:String, value:Any):Void {
        if (name == "complexRelationships") {
            if (_relationshipDefs == null) {
                _relationshipDefs = new RelationshipDefinitions();
            }
            _relationshipDefs.complexRelationships = value;
        }
        _properties.set(name, value);
    }
    public function getProperty(name:String, defaultValue:Any):Any {
        if (_properties == null || !_properties.exists(name)) {
            return defaultValue;
        }
        return _properties.get(name);
    }

    public function new() {
    }

    public function config(details:Dynamic) {
        _config = details;
        // TODO: validate details
    }

    private function createConnection() {
        if (_connection != null) {
            return;
        }
        _connection = new MySqlDatabaseConnection({
            //database: details.database,
            host: _config.host,
            user: _config.user,
            pass: _config.pass
        });
    }

    public function create():Promise<DatabaseResult<IDatabase>> {
        return new Promise((resolve, reject) -> {
            if (_config.database == null) {
                resolve(new DatabaseResult(this));
                return;
            } else {
                _connection.connectionDetails.database = _config.database;
                _connection.exec(buildCreateDatabase(_config.database)).then(response -> {
                    clearCachedSchema();
                    return _connection.query(buildSelectDatabase(_config.database));
                }).then(_ -> {
                    resolve(new DatabaseResult(this));
                }, (error:MySqlError) -> {
                    reject(MySqlError2DatabaseError(error, "delete"));
                });
            }
        });
    }

    public function delete():Promise<DatabaseResult<Bool>> {
        return new Promise((resolve, reject) -> {
            if (_config.database == null) {
                resolve(new DatabaseResult(this, true));
                return;
            } else {
                _connection.exec(buildDropDatabase(_config.database)).then(response -> {
                    clearCachedSchema();
                    resolve(new DatabaseResult(this, true));
                }, (error:MySqlError) -> {
                    reject(MySqlError2DatabaseError(error, "delete"));
                });
            }
        });
    }

    private var _schema:DatabaseSchema = null;
    public function schema():Promise<DatabaseResult<DatabaseSchema>> {
        return new Promise((resolve, reject) -> {
            if (_schema == null) {
                Utils.loadFullDatabaseSchema(_connection, _config, MySqlDataTypeMapper.get()).then(schema -> {
                    _schema = schema;
                    resolve(new DatabaseResult(this, _schema));
                }, (error:MySqlError) -> {
                    reject(MySqlError2DatabaseError(error, "schema"));
                });
            } else {
                resolve(new DatabaseResult(this, _schema));
            }
        });
    }

    public function clearCachedSchema() {
        _schema = null;
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

    public function clearTableRelationships() {
        _relationshipDefs = null;
    }

    public function connect():Promise<DatabaseResult<Bool>> {
        return new Promise((resolve, reject) -> {
            createConnection();
            _connection.open().then(response -> {
                if (_config.database == null) {
                    return null;
                }
                return _connection.query(buildHasDatabase(_config.database));
            }).then(result -> {
                if (result == null || result.data == null || result.data.length == 0) {
                    return null;
                }
                return _connection.query(buildSelectDatabase(_config.database));
            }).then(_ -> {
                resolve(new DatabaseResult(this, true));
            }, (error:MySqlError) -> {
                reject(MySqlError2DatabaseError(error, "connect"));
            });
        });
    }

    public function disconnect():Promise<DatabaseResult<Bool>> {
        return new Promise((resolve, reject) -> {
            _connection.close();
            _connection = null;
            clearCachedSchema();
            resolve(new DatabaseResult(this, true));
        });
    }

    public function table(name:String):Promise<DatabaseResult<ITable>> {
        return new Promise((resolve, reject) -> {
            _connection.get(SQL_TABLE_EXISTS, [_config.database, name]).then(response -> {
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

                _schema = null;
                resolve(new DatabaseResult(this, table));
            }, (error:MySqlError) -> {
                reject(MySqlError2DatabaseError(error, "createTable"));
            });
        });
    }

    public function deleteTable(name:String):Promise<DatabaseResult<Bool>> {
        return new Promise((resolve, reject) -> {
            _schema = null;
            reject(new DatabaseError("not implemented", "deleteTable"));
        });
    }
}