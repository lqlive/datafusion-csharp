# Apache DataFusion C# SDK

This repository contains a C# binding for Apache DataFusion. It is an in-process query engine binding, not an HTTP client SDK.

The managed API lives in `Apache.DataFusion`; the native query engine is a Rust `cdylib` exposed through a small C ABI and called from C# with P/Invoke.

## Build

Build the native library first:

```powershell
cd native
cargo build
```

Optional native features can be enabled with Cargo features:

```powershell
cargo build --features "postgres mysql mongodb clickhouse sqlite"
```

Then build and test the .NET solution:

```powershell
cd ..
dotnet build DataFusion.sln
dotnet test DataFusion.sln
```

For local development, `NativeLibraryLoader` probes `native/target/debug` and `native/target/release` from the repository root.

Native binaries are packaged through RID-specific projects under `packages/`:

```powershell
dotnet pack packages/Apache.DataFusion.Native.win-x64.csproj -c Release
```

Each native package places the binary under `runtimes/{rid}/native/`, matching .NET native asset conventions.

## Quickstart

```csharp
using Apache.DataFusion;

using SessionContext context = new();
using DataFrame dataFrame = context.Sql("SELECT 1 AS value UNION ALL SELECT 2 AS value");

Console.WriteLine(dataFrame.Count());

using DataFrame streamDataFrame = context.Sql("SELECT 'hello' AS message");
using BatchReader reader = streamDataFrame.ExecuteStream();
while (await reader.ReadNextRecordBatchAsync() is { } batch)
{
    Console.WriteLine(batch.Length);
}
```

## Implemented Surface

- `SessionContext`: SQL, configured builder, GetOption, memory usage, runtime stats feature gate, object-store registration options, external table provider registration, Parquet/CSV/JSON/Arrow IPC/Avro register and read.
- `DataFrame`: `Collect`, `ExecuteStream`, `Count`, `Show`, strong Arrow schema access, schema IPC, explain/cache/describe, projection/filter/limit/distinct/drop/rename/with-column/unnest.
- Set operations: union, union distinct, union by name, intersect, except.
- Join operations: column joins and SQL predicate joins.
- Repartition and sort.
- Writes: Parquet, CSV, JSON.
- Plan exchange: DataFusion logical plan proto, Substrait feature gate.
- TableProvider: `SimpleTableProvider` backed by Arrow IPC and native `MemTable`; PostgreSQL, MySQL, MongoDB, ClickHouse, and SQLite backed by `datafusion-table-providers`.
- Scalar UDF: zero-argument Int64 managed callback UDFs.
- Typed native exceptions: plan, execution, IO, resources, configuration, not implemented.

## Current Limits

Object-store AWS/GCS/HTTP backends are implemented behind Cargo features (`object-store-aws`, `object-store-gcp`, `object-store-http`). External table providers are implemented behind Cargo features (`postgres`, `mysql`, `mongodb`, `clickhouse`, `sqlite`). The default native build keeps these disabled to avoid linking larger optional stacks into the base DLL; registering one without the matching feature returns a clear native error.

```csharp
using SessionContext context = new();
context.RegisterPostgres(
    "companies",
    new PostgresTableOptions(
        "host=localhost port=5432 dbname=postgres user=postgres password=password sslmode=disable",
        "companies")
    {
        SchemaName = "public",
    });

context.RegisterMySql(
    "companies_mysql",
    new MySqlTableOptions("mysql://root:password@localhost:3306/mysql_db", "companies"));

context.RegisterMongoDb(
    "companies_mongo",
    new MongoDbTableOptions(
        "mongodb://root:password@localhost:27017/mongo_db?authSource=admin&tls=false",
        "companies"));

context.RegisterClickHouse(
    "reports",
    new ClickHouseTableOptions("http://localhost:8123", "Reports")
    {
        Database = "default",
        User = "default",
    });

context.RegisterSqlite(
    "companies_sqlite",
    new SqliteTableOptions("example.db", "companies"));
```

`Collect` and `ExecuteStream` currently return Arrow IPC-backed readers on the C# side. `BatchReader.Transport` reports `ArrowIpcFallback`. The native C ABI is structured so a direct Arrow C Data Interface path can be added later once the target C# Arrow package exposes the required stable API.
