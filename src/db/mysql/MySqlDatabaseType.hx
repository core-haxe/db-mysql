package db.mysql;

import db.macros.IDatabaseType;
import db.macros.DatabaseTypeInfo;

#if (js && !(hxnodejs))
#error "hxnodejs needed for js builds"
#end

class MySqlDatabaseType implements IDatabaseType {
    public function new() {
    }

    public function typeInfo():DatabaseTypeInfo {
        return {
            ctor: MySqlDatabase.new
        };
    }
}