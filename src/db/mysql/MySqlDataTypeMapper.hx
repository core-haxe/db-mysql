package db.mysql;

import haxe.io.Bytes;

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

    public function shouldConvertValueToDatabase(value:Any):Bool {
        if ((value is Bytes)) {
            return true;
        }
        return false;
    }

    public function convertValueToDatabase(value:Any):Any {
        if ((value is Bytes)) {
            var bytes:Bytes = cast value;
            return bytes.toString(); // TODO, is this right? Test with binary image or something
        }
        return value;
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