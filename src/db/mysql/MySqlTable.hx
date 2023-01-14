package db.mysql;

import promises.PromiseUtils;
import promises.Promise;
import mysql.DatabaseConnection as MySqlDatabaseConnection;
import mysql.MySqlError;
import db.mysql.Utils.*;
import db.utils.SqlUtils.*;
import db.Query.QueryExpr;

class MySqlTable implements ITable {
    public var db:IDatabase;
    public var name:String;
    public var exists:Bool;

    public function new(db:IDatabase) {
        this.db = db;
    }

    public function all():Promise<DatabaseResult<Array<Record>>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'all'));
                return;
            }
            connection.all(buildSelect(this)).then(response -> {
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

    public function page(pageIndex:Int, pageSize:Int = 100, query:QueryExpr = null):Promise<DatabaseResult<Array<Record>>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'page'));
                return;
            }
            reject(new DatabaseError("not implemented", "page"));
        });
    }

    public function add(record:Record):Promise<DatabaseResult<Record>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'add'));
                return;
            }

            var values = [];
            var sql = buildInsert(this, record, values);
            connection.get(sql, values).then(response -> {
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
            var values = [];
            var sql = buildDeleteRecord(this, record, values);
            connection.get(sql, values).then(response -> {
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
            connection.exec(buildDeleteWhere(this, query)).then(response -> {
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
            var values = [];
            var sql = buildUpdate(this, query, record, values);
            connection.get(sql, values).then(response -> {
                resolve(new DatabaseResult(db, this, record));
            }, (error:MySqlError) -> {
                reject(MySqlError2DatabaseError(error, "update"));
            });
        });
    }

    public function find(query:QueryExpr):Promise<DatabaseResult<Array<Record>>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'find'));
                return;
            }
            var values = [];
            var sql = buildSelect(this, query, null, values, db.definedTableRelationships());
            connection.all(sql, values).then(response -> {
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

    public function findOne(query:QueryExpr):Promise<DatabaseResult<Record>> {
        return new Promise((resolve, reject) -> {
            if (!exists) {
                reject(new DatabaseError('table "${name}" does not exist', 'findOne'));
                return;
            }
            var sql = buildSelect(this, query, 1, null, db.definedTableRelationships());
            connection.get(sql).then(response -> {
                resolve(new DatabaseResult(db, this, Record.fromDynamic(response.data)));
            }, (error:MySqlError) -> {
                reject(MySqlError2DatabaseError(error, "connect"));
            });
        });
    }

    private var connection(get, null):MySqlDatabaseConnection;
    private function get_connection():MySqlDatabaseConnection {
        return @:privateAccess cast(db, MySqlDatabase)._connection;
    }
}