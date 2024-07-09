package db.mysql;

import logging.LogManager;
import promises.PromiseUtils;
import promises.Promise;
import mysql.DatabaseConnection as MySqlDatabaseConnection;
import mysql.MySqlError;
import db.mysql.Utils.*;
import db.utils.SqlUtils.*;
import Query.QueryExpr;
import logging.Logger;

class MySqlTable implements ITable {
    private static var log = new Logger(MySqlTable, true);

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

            log.beginMeasure("applying schema");
            log.debug("applying schema");
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
                log.endMeasure("applying schema");
                resolve(new DatabaseResult(db, this, newSchema));
            }, (error:DatabaseError) -> {
                log.error("error applying schema");
                reject(error);
            });
        });
    }

    public function all():Promise<DatabaseResult<RecordSet>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'all'));
                return;
            }

            log.beginMeasure("all");
            log.debug("all");
            refreshSchema().then(schemaResult -> {
                var values = [];
                var sql = buildSelect(this, null, null, null, values, db.definedTableRelationships(), schemaResult.data);
                return connection.all(sql);
            }).then(response -> {
                var records:RecordSet = [];
                for (item in response.data) {
                    records.push(Record.fromDynamic(item));
                }
                log.endMeasure("all");
                resolve(new DatabaseResult(db, this, records));
            }, (error:MySqlError) -> {
                log.error("all", error);
                reject(MySqlError2DatabaseError(error, "all"));
            });
        });
    }

    public function page(pageIndex:Int, pageSize:Int = 100, query:QueryExpr = null, allowRelationships:Bool = true):Promise<DatabaseResult<RecordSet>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'find'));
                return;
            }

            log.beginMeasure("page");
            log.debug("page", [pageIndex, pageSize]);
            refreshSchema().then(schemaResult -> {
                var relationshipDefinintions = db.definedTableRelationships();
                if (!allowRelationships) {
                    relationshipDefinintions = null;
                }
                var values = [];
                var sql = buildSelect(this, null, pageSize, pageIndex * pageSize, values, relationshipDefinintions, schemaResult.data);
                return connection.all(sql, values);
            }).then(response -> {
                var records:RecordSet = [];
                for (item in response.data) {
                    records.push(Record.fromDynamic(item));
                }
                log.endMeasure("page");
                resolve(new DatabaseResult(db, this, records));
            }, (error:MySqlError) -> {
                log.error("page", error);
                reject(MySqlError2DatabaseError(error, "page"));
            });
        });
    }

    public function add(record:Record):Promise<DatabaseResult<Record>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'add'));
                return;
            }

            log.beginMeasure("add");
            if (LogManager.instance.shouldLogDebug) {
                log.debug("add", record.debugString());
            }
            var insertedId:Int = -1;
            var schema:DatabaseSchema = null;
            refreshSchema(true).then(schemaResult -> {
                schema = schemaResult.data;
                var values = [];
                var sql = buildInsert(this, record, values, MySqlDataTypeMapper.get());
                return connection.get(sql, values);
            }).then(response -> {
                insertedId = response.data.insertId;

                var tableSchema = schema.findTable(this.name);
                if (tableSchema != null) {
                    var primaryKeyColumns = tableSchema.findPrimaryKeyColumns();
                    if (primaryKeyColumns.length == 1) { // we'll only "auto set" the primary key column if there is _only_ one of them
                        record.field(primaryKeyColumns[0].name, insertedId);
                    }
                }

                record.field("_insertedId", insertedId);
                log.endMeasure("add");
                resolve(new DatabaseResult(db, this, record));
            }, (error:MySqlError) -> {
                log.error("add", error);
                reject(MySqlError2DatabaseError(error, "add"));
            });
        });
    }

    public function addAll(records:RecordSet):Promise<DatabaseResult<RecordSet>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'addAll'));
                return;
            }

            log.beginMeasure("addAll");
            log.debug("addAll");
            var promises = [];
            for (record in records) {
                promises.push(add.bind(record));
            }

            PromiseUtils.runSequentially(promises).then(results -> {
                log.endMeasure("addAll");
                resolve(new DatabaseResult(db, this, records));
            }, (error:MySqlError) -> {
                log.error("addAll", error);
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

            log.beginMeasure("delete");
            if (LogManager.instance.shouldLogDebug) {
                log.debug("delete", record.debugString());
            }
            refreshSchema().then(schemaResult -> {
                var values = [];
                var sql = buildDeleteRecord(this, record, values);
                return connection.get(sql, values);
            }).then(response -> {
                log.endMeasure("delete");
                resolve(new DatabaseResult(db, this, record));
            }, (error:MySqlError) -> {
                log.error("delete", error);
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

            log.beginMeasure("deleteAll");
            refreshSchema().then(schemaResult -> {
                return connection.exec(buildDeleteWhere(this, query));
            }).then(response -> {
                log.endMeasure("deleteAll");
                resolve(new DatabaseResult(db, this, true));
            }, (error:MySqlError) -> {
                log.error("deleteAll", error);
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

            log.beginMeasure("update");
            if (LogManager.instance.shouldLogDebug) {
                log.debug("update", record.debugString());
            }
            refreshSchema().then(schemaResult -> {
                var values = [];
                var sql = buildUpdate(this, query, record, values, MySqlDataTypeMapper.get());
                return connection.get(sql, values);
            }).then(response -> {
                log.endMeasure("update");
                resolve(new DatabaseResult(db, this, record));
            }, (error:MySqlError) -> {
                log.error("update", error);
                reject(MySqlError2DatabaseError(error, "update"));
            });
        });
    }

    public function find(query:QueryExpr, allowRelationships:Bool = true):Promise<DatabaseResult<RecordSet>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'find'));
                return;
            }

            log.beginMeasure("find");
            log.debug("find");
            refreshSchema().then(schemaResult -> {
                var relationshipDefinintions = db.definedTableRelationships();
                if (!allowRelationships) {
                    relationshipDefinintions = null;
                }
                var values = [];
                var sql = buildSelect(this, query, null, null, values, relationshipDefinintions, schemaResult.data);
                return connection.all(sql, values);
            }).then(response -> {
                var records:RecordSet = [];
                for (item in response.data) {
                    records.push(Record.fromDynamic(item));
                }
                log.endMeasure("find");
                resolve(new DatabaseResult(db, this, records));
            }, (error:MySqlError) -> {
                log.error("find");
                reject(MySqlError2DatabaseError(error, "find"));
            });
        });
    }

    public function findOne(query:QueryExpr, allowRelationships:Bool = true):Promise<DatabaseResult<Record>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'findOne'));
                return;
            }

            log.beginMeasure("findOne");
            log.debug("findOne");
            refreshSchema().then(schemaResult -> {
                var relationshipDefinintions = db.definedTableRelationships();
                if (!allowRelationships) {
                    relationshipDefinintions = null;
                }
                var values = [];
                var sql = buildSelect(this, query, 1, values, relationshipDefinintions, schemaResult.data);
                return connection.get(sql, values);
            }).then(response -> {
                var record:Record = null;
                if (response.data != null && (response.data is Array)) {
                    record = Record.fromDynamic(response.data[0]);
                } else if (response.data != null) {
                    record =  Record.fromDynamic(response.data);
                }
                log.endMeasure("findOne");
                resolve(new DatabaseResult(db, this, record));
            }, (error:MySqlError) -> {
                log.error("findOne", error);
                reject(MySqlError2DatabaseError(error, "findOne"));
            });
        });
    }

    public function findUnique(columnName:String, query:QueryExpr = null, allowRelationships:Bool = true):Promise<DatabaseResult<RecordSet>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'findOne'));
                return;
            }

            log.beginMeasure("findUnique");
            log.debug("findUnique");
            refreshSchema().then(schemaResult -> {
                var relationshipDefinintions = db.definedTableRelationships();
                if (!allowRelationships) {
                    relationshipDefinintions = null;
                }
                var values = [];
                var sql = buildDistinctSelect(this, query, columnName, null, null, values, relationshipDefinintions, schemaResult.data);
                return connection.all(sql, values);
            }).then(response -> {
                var records:RecordSet = [];
                for (item in response.data) {
                    records.push(Record.fromDynamic(item));
                }
                log.endMeasure("findUnique");
                resolve(new DatabaseResult(db, this, records));
            }, (error:MySqlError) -> {
                log.error("findUnique", error);
                reject(MySqlError2DatabaseError(error, "findUnique"));
            });
        });
    }

    public function count(query:QueryExpr = null):Promise<DatabaseResult<Int>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'findOne'));
                return;
            }

            log.beginMeasure("count");
            log.debug("count");
            refreshSchema().then(schemaResult -> {
                var sql = buildCount(this, query);
                return connection.get(sql);
            }).then(response -> {
                var record = Record.fromDynamic(response.data);
                log.endMeasure("count");
                resolve(new DatabaseResult(db, this, cast record.values()[0]));
            }, (error:MySqlError) -> {
                log.error("count", error);
                reject(MySqlError2DatabaseError(error, "count"));
            });
        });
    }

    public function addColumn(column:ColumnDefinition):Promise<DatabaseResult<Bool>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'addColumn'));
                return;
            }

            log.beginMeasure("addColumn");
            log.debug("addColumn");
            var sql = buildAddColumns(this.name, [column], MySqlDataTypeMapper.get());
            connection.exec(sql).then(result -> {
                clearCachedSchema();
                cast(db, MySqlDatabase).clearCachedSchema();
                log.endMeasure("addColumn");
                resolve(new DatabaseResult(db, this, true));
            }, (error:MySqlError) -> {
                log.error("addColumn", error);
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

            log.beginMeasure("removeColumn");
            log.debug("removeColumn");
            var sql = buildRemoveColumns(this.name, [column], MySqlDataTypeMapper.get());
            connection.exec(sql).then(result -> {
                clearCachedSchema();
                cast(db, MySqlDatabase).clearCachedSchema();
                log.endMeasure("removeColumn");
                resolve(new DatabaseResult(db, this, true));
            }, (error:MySqlError) -> {
                log.error("removeColumn", error);
                reject(MySqlError2DatabaseError(error, "addColumn"));
            });
        });
    }

    #if allow_raw
    public function raw(data:String, values:Array<Any> = null):Promise<DatabaseResult<RecordSet>> {
        return new Promise((resolve, reject) -> {
            if (values == null) {
                values = [];
            }
            var sql = data;
            connection.all(sql, values).then(response -> {
                var records:RecordSet = [];
                for (item in response.data) {
                    records.push(Record.fromDynamic(item));
                }
                resolve(new DatabaseResult(db, this, records));
            }, (error:MySqlError) -> {
                reject(MySqlError2DatabaseError(error, "raw"));
            });
        });
    }
    #end

    private var connection(get, null):MySqlDatabaseConnection;
    private function get_connection():MySqlDatabaseConnection {
        return @:privateAccess cast(db, MySqlDatabase)._connection;
    }

    private function refreshSchema(force:Bool = false):Promise<DatabaseResult<DatabaseSchema>> { // we'll only refresh the data schema if there are table relationships, since the queries might need them
        return new Promise((resolve, reject) -> {
            var alwaysAliasResultFields:Bool = this.db.getProperty("alwaysAliasResultFields", false);
            if (force == false && alwaysAliasResultFields == false && db.definedTableRelationships() == null) {
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