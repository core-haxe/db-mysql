package db.mysql;

import mysql.MySqlError;
import promises.Promise;
import mysql.DatabaseConnection as MySqlDatabaseConnection;
import db.mysql.Utils.*;
import logging.Logger;

class MySqlDatabase implements IDatabase {
    private static var log = new Logger(MySqlDatabase, true);

    private var _connection:MySqlDatabaseConnection = null;
    private var _relationshipDefs:RelationshipDefinitions = null;
    private var _config:Dynamic;

    public function new() {
    }

    private var _properties:Map<String, Any> = [];
    public function setProperty(name:String, value:Any):Void {
        if (name == "complexRelationships") {
            if (_relationshipDefs == null) {
                _relationshipDefs = new RelationshipDefinitions();
            }
            _relationshipDefs.complexRelationships = value;
        }

        log.debug("setting property", [name, value]);

        _properties.set(name, value);
    }
    public function getProperty(name:String, defaultValue:Any):Any {
        if (_properties == null || !_properties.exists(name)) {
            return defaultValue;
        }
        return _properties.get(name);
    }

    public function config(details:Dynamic) {
        _config = details;
        // TODO: validate details

        log.debug("config", _config);
    }

    private function createConnection() {
        if (_connection != null) {
            return;
        }

        log.debug("creating connection");

        var port = 3306;
        if (_config.port != null) {
            port = _config.port;
        }
        _connection = new MySqlDatabaseConnection({
            //database: details.database,
            host: _config.host,
            port: port,
            user: _config.user,
            pass: _config.pass
        });
        var autoReconnect = getProperty("autoReconnect", null);
        var autoReconnectIntervalMS = getProperty("autoReconnectIntervalMS", null);
        var replayQueriesOnReconnection = getProperty("replayQueriesOnReconnection", null);
        if (autoReconnect != null) {
            _connection.autoReconnect = autoReconnect;
        }
        if (autoReconnectIntervalMS != null) {
            _connection.autoReconnectIntervalMS = autoReconnectIntervalMS;
        }
        if (replayQueriesOnReconnection != null) {
            _connection.replayQueriesOnReconnection = replayQueriesOnReconnection;
        }
    }

    public function create():Promise<DatabaseResult<IDatabase>> {
        return new Promise((resolve, reject) -> {
            if (_config.database == null) {
                resolve(new DatabaseResult(this));
                return;
            } else {
                log.beginMeasure("create " + _config.database);
                log.debug("creating database:", _config.database);
                _connection.connectionDetails.database = _config.database;
                _connection.exec(buildCreateDatabase(_config.database)).then(response -> {
                    clearCachedSchema();
                    return _connection.query(buildSelectDatabase(_config.database));
                }).then(_ -> {
                    log.endMeasure("create " + _config.database);
                    resolve(new DatabaseResult(this));
                }, (error:MySqlError) -> {
                    log.endMeasure("create " + _config.database);
                    log.error("error creating database:", error);
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
                log.beginMeasure("delete " + _config.database);
                log.debug("deleting database:", _config.database);
                _connection.exec(buildDropDatabase(_config.database)).then(response -> {
                    clearCachedSchema();
                    log.endMeasure("delete " + _config.database);
                    resolve(new DatabaseResult(this, true));
                }, (error:MySqlError) -> {
                    log.endMeasure("delete " + _config.database);
                    log.error("error deleting database:", error);
                    reject(MySqlError2DatabaseError(error, "delete"));
                });
            }
        });
    }

    private var _schema:DatabaseSchema = null;
    public function schema(force:Bool = false):Promise<DatabaseResult<DatabaseSchema>> {
        return new Promise((resolve, reject) -> {
            if (force) {
                clearCachedSchema();
            }
            if (_schema == null) {
                log.beginMeasure("schema");
                log.debug("loading database schema for:", _config.database);
                Utils.loadFullDatabaseSchema(_connection, _config, MySqlDataTypeMapper.get()).then(schema -> {
                    _schema = schema;
                    log.endMeasure("schema");
                    resolve(new DatabaseResult(this, _schema));
                }, (error:MySqlError) -> {
                    log.endMeasure("schema");
                    log.error("error loading database schema:", error);
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
        log.debug("defining relationship", [field1, field2]);
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
            log.beginMeasure("connect");
            log.debug("connecting");
            createConnection();
            _connection.open().then(response -> {
                if (_config.database == null) {
                    return null;
                }
                log.debug("checking for database:", _config.database);
                return _connection.query(buildHasDatabase(_config.database));
            }).then(result -> {
                if (result == null || result.data == null || result.data.length == 0) {
                    return null;
                }
                log.debug("database exists:", _config.database);
                return _connection.query(buildSelectDatabase(_config.database));
            }).then(_ -> {
                log.endMeasure("connect");
                _connection.connectionDetails.database = _config.database;
                resolve(new DatabaseResult(this, true));
            }, (error:MySqlError) -> {
                log.endMeasure("connect");
                log.error("error connecting:", error);
                reject(MySqlError2DatabaseError(error, "connect"));
            });
        });
    }

    public function disconnect():Promise<DatabaseResult<Bool>> {
        return new Promise((resolve, reject) -> {
            log.debug("disconnecting");
            _connection.close();
            _connection = null;
            clearCachedSchema();
            resolve(new DatabaseResult(this, true));
        });
    }

    public function table(name:String):Promise<DatabaseResult<ITable>> {
        return new Promise((resolve, reject) -> {
            log.beginMeasure("table " + name);
            log.debug("looking for table:", [_config.database, name]);
            _connection.get(SQL_TABLE_EXISTS, [_config.database, name]).then(response -> {
                var table:ITable = new MySqlTable(this);
                table.name = name;
                table.exists = !(response.data == null);
                log.endMeasure("table " + name);
                if (table.exists) {
                    log.debug("table found:", [_config.database, name]);
                } else {
                    log.debug("table not found:", [_config.database, name]);
                }
                resolve(new DatabaseResult(this, table, table));
            }, (error:MySqlError) -> {
                log.endMeasure("table " + name);
                log.error("error looking for table:", error);
                reject(MySqlError2DatabaseError(error, "table"));
            });
        });
    }

    public function createTable(name:String, columns:Array<ColumnDefinition>):Promise<DatabaseResult<ITable>> {
        return new Promise((resolve, reject) -> {
            log.beginMeasure("createTable " + name);
            log.debug("creating table", [name, columns]);
            var sql = buildCreateTable(name, columns, MySqlDataTypeMapper.get());
            _connection.exec(sql).then(response -> {
                var table:ITable = new MySqlTable(this);
                table.name = name;
                table.exists = true;

                log.endMeasure("createTable " + name);
                _schema = null;
                resolve(new DatabaseResult(this, table, table));
            }, (error:MySqlError) -> {
                log.endMeasure("createTable " + name);
                log.error("error creating table", error);
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

    #if allow_raw
    public function raw(data:String, values:Array<Any> = null):Promise<DatabaseResult<RecordSet>> {
        return new Promise((resolve, reject) -> {
            if (values == null) {
                values = [];
            }
            var sql = data;
            _connection.all(sql, values).then(response -> {
                var records:RecordSet = [];
                for (item in response.data) {
                    records.push(Record.fromDynamic(item));
                }
                resolve(new DatabaseResult(this, records));
            }, (error:MySqlError) -> {
                reject(MySqlError2DatabaseError(error, "raw"));
            });
        });
    }
    #end
}
