# db-mysql
mysql database plugin for [__db-core__](https://github.com/core-haxe/db-core)

# basic usage

```haxe
var db:IDatabase = DatabaseFactory.createDatabase(DatabaseFactory.MYSQL, {
    database: "somedb",
    host: "localhost",
    user: "someuser",
    pass: "somepassword"
});
...
```

See [__db-core__](https://github.com/core-haxe/db-core) for further information on how to use `IDatabase`

_Note: the act of including this haxelib in your project automatically registers its type with `DatabaseFactory`_