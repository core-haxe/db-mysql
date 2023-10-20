package db.mysql;

class MySqlDataTypeMapper implements IDataTypeMapper {
    private static var _instance:IDataTypeMapper = null;
    public static function get():IDataTypeMapper {
        if (_instance == null) {
            _instance = new MySqlDataTypeMapper();
        }
        return _instance;
    }

    ///////////////////////////////////////////////////////////////////////////////////////

    public function new() {
    }

    public function haxeTypeToDatabaseType(haxeType:ColumnType):String {
        return switch (haxeType) {
            case Number:        'INT';
            case Decimal:       'DECIMAL';
            case Boolean:       'INT';
            case Text(n):       'VARCHAR($n)';
            case Memo:          'TEXT';
            case Binary:        'BLOB';
            case Unknown:       'TEXT';
        }
    }
}