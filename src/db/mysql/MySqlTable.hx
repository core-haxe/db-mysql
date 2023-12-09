package db.mysql;

import promises.PromiseUtils;
import promises.Promise;
import mysql.DatabaseConnection as MySqlDatabaseConnection;
import mysql.MySqlError;
import db.mysql.Utils.*;
import db.utils.SqlUtils.*;
import Query.QueryExpr;

class MySqlTable implements ITable {
    public var db:IDatabase;
    public var name:String;
    public var exists:Bool;

    public function new(db:IDatabase) {
        this.db = db;
    }

    private var _tableSchema:TableSchema = null;
    public function schema():Promise<DatabaseResult<TableSchema>> {
        return new Promise((resolve, reject) -> {
            if (_tableSchema != null) {
                resolve(new DatabaseResult(db, this, _tableSchema));
                return;
            }

            this.db.schema().then(result -> {
                _tableSchema = result.data.findTable(this.name);
                resolve(new DatabaseResult(db, this, _tableSchema));
            }, (error:DatabaseError) -> {
                reject(error);
            });
        });
    }

    public function clearCachedSchema() {
        _tableSchema = null;
    }

    public function applySchema(newSchema:TableSchema):Promise<DatabaseResult<TableSchema>> {
        return new Promise((resolve, reject) -> {
            
            var schemaChanged:Bool = false;

            schema().then(result -> {
                var promises = [];
                var currentSchema = result.data;
                if (currentSchema != null && !currentSchema.equals(newSchema)) {
                    var diff = currentSchema.diff(newSchema);

                    for (added in diff.addedColumns) {
                        promises.push(addColumn.bind(added));
                        schemaChanged = true;
                    }

                    for (removed in diff.removedColumns) {
                        promises.push(removeColumn.bind(removed));
                        schemaChanged = true;
                    }
                }
                return PromiseUtils.runSequentially(promises);
            }).then(result -> {
                if (schemaChanged) {
                    clearCachedSchema();
                    cast(db, MySqlDatabase).clearCachedSchema();
                }
                resolve(new DatabaseResult(db, this, newSchema));
            }, (error:DatabaseError) -> {
                reject(error);
            });
        });
    }

    public function all():Promise<DatabaseResult<Array<Record>>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'all'));
                return;
            }

            refreshSchema().then(schemaResult -> {
                var values = [];
                var sql = buildSelect(this, null, null, null, values, db.definedTableRelationships(), schemaResult.data);
                return connection.all(sql);
            }).then(response -> {
                var records = [];
                for (item in response.data) {
                    records.push(Record.fromDynamic(item));
                }
                resolve(new DatabaseResult(db, this, records));
            }, (error:MySqlError) -> {
                reject(MySqlError2DatabaseError(error, "connect"));
            });
        });
    }

    public function page(pageIndex:Int, pageSize:Int = 100, query:QueryExpr = null, allowRelationships:Bool = true):Promise<DatabaseResult<Array<Record>>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'find'));
                return;
            }

            refreshSchema().then(schemaResult -> {
                var relationshipDefinintions = db.definedTableRelationships();
                if (!allowRelationships) {
                    relationshipDefinintions = null;
                }
                var values = [];
                var sql = buildSelect(this, null, pageSize, pageIndex * pageSize, values, relationshipDefinintions, schemaResult.data);
                return connection.all(sql, values);
            }).then(response -> {
                var records = [];
                for (item in response.data) {
                    records.push(Record.fromDynamic(item));
                }
                resolve(new DatabaseResult(db, this, records));
            }, (error:MySqlError) -> {
                reject(MySqlError2DatabaseError(error, "connect"));
            });
        });
    }

    public function add(record:Record):Promise<DatabaseResult<Record>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'add'));
                return;
            }

            var insertedId:Int = -1;
            refreshSchema().then(schemaResult -> {
                var values = [];
                var sql = buildInsert(this, record, values, MySqlDataTypeMapper.get());
                return connection.get(sql, values);
            }).then(response -> {
                insertedId = response.data.insertId;
                record.field("_insertedId", insertedId);
                resolve(new DatabaseResult(db, this, record));
            }, (error:MySqlError) -> {
                reject(MySqlError2DatabaseError(error, "add"));
            });
        });
    }

    public function addAll(records:Array<Record>):Promise<DatabaseResult<Array<Record>>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'addAll'));
                return;
            }

            var promises = [];
            for (record in records) {
                promises.push(add.bind(record));
            }

            PromiseUtils.runSequentially(promises).then(results -> {
                resolve(new DatabaseResult(db, this, records));
            }, (error:MySqlError) -> {
                reject(MySqlError2DatabaseError(error, "addAll"));
            });
        });
    }

    public function delete(record:Record):Promise<DatabaseResult<Record>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'delete'));
                return;
            }

            refreshSchema().then(schemaResult -> {
                var values = [];
                var sql = buildDeleteRecord(this, record, values);
                return connection.get(sql, values);
            }).then(response -> {
                resolve(new DatabaseResult(db, this, record));
            }, (error:MySqlError) -> {
                reject(MySqlError2DatabaseError(error, "delete"));
            });
        });
    }

    public function deleteAll(query:QueryExpr = null):Promise<DatabaseResult<Bool>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'deleteAll'));
                return;
            }

            refreshSchema().then(schemaResult -> {
                return connection.exec(buildDeleteWhere(this, query));
            }).then(response -> {
                resolve(new DatabaseResult(db, this, true));
            }, (error:MySqlError) -> {
                reject(MySqlError2DatabaseError(error, "deleteAll"));
            });
        });
    }

    public function update(query:QueryExpr, record:Record):Promise<DatabaseResult<Record>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'update'));
                return;
            }

            refreshSchema().then(schemaResult -> {
                var values = [];
                var sql = buildUpdate(this, query, record, values, MySqlDataTypeMapper.get());
                return connection.get(sql, values);
            }).then(response -> {
                resolve(new DatabaseResult(db, this, record));
            }, (error:MySqlError) -> {
                reject(MySqlError2DatabaseError(error, "update"));
            });
        });
    }

    public function find(query:QueryExpr, allowRelationships:Bool = true):Promise<DatabaseResult<Array<Record>>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'find'));
                return;
            }

            refreshSchema().then(schemaResult -> {
                var relationshipDefinintions = db.definedTableRelationships();
                if (!allowRelationships) {
                    relationshipDefinintions = null;
                }
                var values = [];
                var sql = buildSelect(this, query, null, values, relationshipDefinintions, schemaResult.data);
                return connection.all(sql, values);
            }).then(response -> {
                var records = [];
                for (item in response.data) {
                    records.push(Record.fromDynamic(item));
                }
                resolve(new DatabaseResult(db, this, records));
            }, (error:MySqlError) -> {
                reject(MySqlError2DatabaseError(error, "connect"));
            });
        });
    }

    public function findOne(query:QueryExpr, allowRelationships:Bool = true):Promise<DatabaseResult<Record>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'findOne'));
                return;
            }

            refreshSchema().then(schemaResult -> {
                var relationshipDefinintions = db.definedTableRelationships();
                if (!allowRelationships) {
                    relationshipDefinintions = null;
                }
                var sql = buildSelect(this, query, 1, null, relationshipDefinintions, schemaResult.data);
                return connection.get(sql);
            }).then(response -> {
                var record:Record = null;
                if (response.data != null && (response.data is Array)) {
                    record = Record.fromDynamic(response.data[0]);
                } else if (response.data != null) {
                    record =  Record.fromDynamic(response.data);
                }
                resolve(new DatabaseResult(db, this, record));
            }, (error:MySqlError) -> {
                reject(MySqlError2DatabaseError(error, "connect"));
            });
        });
    }

    public function findUnique(columnName:String, query:QueryExpr = null, allowRelationships:Bool = true):Promise<DatabaseResult<Array<Record>>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'findOne'));
                return;
            }

            refreshSchema().then(schemaResult -> {
                var relationshipDefinintions = db.definedTableRelationships();
                if (!allowRelationships) {
                    relationshipDefinintions = null;
                }
                var values = [];
                var sql = buildDistinctSelect(this, query, columnName, null, null, values, relationshipDefinintions, schemaResult.data);
                return connection.all(sql, values);
            }).then(response -> {
                var records = [];
                for (item in response.data) {
                    records.push(Record.fromDynamic(item));
                }
                resolve(new DatabaseResult(db, this, records));
            }, (error:MySqlError) -> {
                reject(MySqlError2DatabaseError(error, "connect"));
            });
        });
    }

    public function count(query:QueryExpr = null):Promise<DatabaseResult<Int>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'findOne'));
                return;
            }

            refreshSchema().then(schemaResult -> {
                var sql = buildCount(this, query);
                return connection.get(sql);
            }).then(response -> {
                var record = Record.fromDynamic(response.data);
                resolve(new DatabaseResult(db, this, cast record.values()[0]));
            }, (error:MySqlError) -> {
                reject(MySqlError2DatabaseError(error, "connect"));
            });
        });
    }

    public function addColumn(column:ColumnDefinition):Promise<DatabaseResult<Bool>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'addColumn'));
                return;
            }

            var sql = buildAddColumns(this.name, [column], MySqlDataTypeMapper.get());
            connection.exec(sql).then(result -> {
                clearCachedSchema();
                cast(db, MySqlDatabase).clearCachedSchema();
                resolve(new DatabaseResult(db, this, true));
            }, (error:MySqlError) -> {
                reject(MySqlError2DatabaseError(error, "addColumn"));
            });
        });
    }

    public function removeColumn(column:ColumnDefinition):Promise<DatabaseResult<Bool>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'addColumn'));
                return;
            }

            var sql = buildRemoveColumns(this.name, [column], MySqlDataTypeMapper.get());
            connection.exec(sql).then(result -> {
                clearCachedSchema();
                cast(db, MySqlDatabase).clearCachedSchema();
                resolve(new DatabaseResult(db, this, true));
            }, (error:MySqlError) -> {
                reject(MySqlError2DatabaseError(error, "addColumn"));
            });
        });
    }

    private var connection(get, null):MySqlDatabaseConnection;
    private function get_connection():MySqlDatabaseConnection {
        return @:privateAccess cast(db, MySqlDatabase)._connection;
    }

    private function refreshSchema():Promise<DatabaseResult<DatabaseSchema>> { // we'll only refresh the data schema if there are table relationships, since the queries might need them
        return new Promise((resolve, reject) -> {
            var alwaysAliasResultFields:Bool = this.db.getProperty("alwaysAliasResultFields", false);
            if (alwaysAliasResultFields == false && db.definedTableRelationships() == null) {
                resolve(new DatabaseResult(db, this, null));
                return;
            }

            db.schema().then(result -> {
                resolve(result);
            }, (error) -> {
                reject(error);
            });
        });
    }
}