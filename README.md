# Apache DataFusion C# SDK

A C# binding for [Apache DataFusion](https://datafusion.apache.org/). This is an
**in-process query engine** binding (think SQLite/DuckDB-style embedding), not an
HTTP client SDK.

The managed API lives in the `Apache.DataFusion` namespace. The query engine itself
is the Rust DataFusion core, compiled to a `cdylib` and exposed through a small C ABI
that C# calls via P/Invoke. Configuration crosses the boundary as Protobuf messages,
and result sets cross as Apache Arrow IPC byte streams.

```
┌──────────────────────┐   P/Invoke (C ABI)   ┌───────────────────────────┐
│  Apache.DataFusion    │ ───────────────────► │  datafusion_csharp_native │
│  (managed C# API)     │   Protobuf options   │  (Rust cdylib)            │
│                       │ ◄─────────────────── │  DataFusion + Tokio       │
└──────────────────────┘   Arrow IPC results  └───────────────────────────┘
```

## Repository layout

| Path | Contents |
| --- | --- |
| `src/Apache.DataFusion/` | Managed SDK (public API + interop) |
| `native/` | Rust `cdylib` exposing the C ABI |
| `proto/` | Protobuf contracts shared by C# and Rust |
| `tests/Apache.DataFusion.Tests/` | xUnit test suite |
| `examples/Apache.DataFusion.Sample.*/` | Runnable example projects (one per scenario) |
| `packages/` | RID-specific native NuGet packaging projects |

## Prerequisites

- **.NET SDK 9.0.200+** &mdash; required to build the `.slnx` solution format
  (pinned to `9.0.301` in `global.json`). The libraries still target `net8.0`.
- **Rust toolchain** (stable) &mdash; to build the native `cdylib`.

## Install

Published on NuGet as a managed package plus RID-specific native runtime packages.
Install the managed package and **the one native package matching your platform**:

```powershell
dotnet add package Apache.DataFusion
dotnet add package Apache.DataFusion.Native.win-x64
```

The managed `Apache.DataFusion` package intentionally does **not** depend on the native
packages. If it depended on all of them, every platform's binary (hundreds of MB total)
would be copied into your build output. Instead you pick exactly the runtime you need:

| Platform | Native package |
| --- | --- |
| Windows x64 | `Apache.DataFusion.Native.win-x64` |
| Linux x64 (glibc) | `Apache.DataFusion.Native.linux-x64` |
| Linux arm64 (glibc) | `Apache.DataFusion.Native.linux-arm64` |
| Linux x64 (musl / Alpine) | `Apache.DataFusion.Native.linux-musl-x64` |
| Linux arm64 (musl / Alpine) | `Apache.DataFusion.Native.linux-musl-arm64` |
| macOS x64 (Intel) | `Apache.DataFusion.Native.osx-x64` |
| macOS arm64 (Apple Silicon) | `Apache.DataFusion.Native.osx-arm64` |

If you build for multiple platforms (e.g. CI matrix or cross-publish), add each target's
native package; only the matching RID's binary is kept on `dotnet publish -r <rid>`.

Source and issues: [github.com/lqlive/datafusion-csharp](https://github.com/lqlive/datafusion-csharp).

## Build

Clone the repository:

```powershell
git clone https://github.com/lqlive/datafusion-csharp.git
cd datafusion-csharp
```

Build the native library first:

```powershell
cd native
cargo build
```

Optional native features are opt-in via Cargo features (see
[Optional native features](#optional-native-features)):

```powershell
cargo build --features "postgres mysql mongodb clickhouse sqlite"
```

Then build and test the .NET solution:

```powershell
cd ..
dotnet build datafusion-csharp.slnx
dotnet test datafusion-csharp.slnx
```

For local development, `NativeLibraryLoader` probes `native/target/debug` and
`native/target/release` upward from the application base directory, so the tests and
examples pick up a freshly built native library automatically.

## Quickstart

```csharp
using Apache.DataFusion;

using SessionContext context = new();
using DataFrame dataFrame = context.Sql("SELECT 1 AS value UNION ALL SELECT 2 AS value");

Console.WriteLine(dataFrame.Count());

using DataFrame stream = context.Sql("SELECT 'hello' AS message");
using ArrowBatchReader reader = stream.ExecuteStream();
while (await reader.ReadNextRecordBatchAsync() is { } batch)
{
    Console.WriteLine(batch.Length);
}
```

## Examples

Each scenario is an independent, runnable console project under `examples/`:

| Project | Demonstrates |
| --- | --- |
| `Apache.DataFusion.Sample.Sql` | Register a CSV file and run a SQL query |
| `Apache.DataFusion.Sample.DataFrame` | Fluent DataFrame API (`Filter`/`Select`/`WithColumn`/`Sort`) |
| `Apache.DataFusion.Sample.Join` | Inner join across two CSV sources |
| `Apache.DataFusion.Sample.SetOperations` | `UnionDistinct`/`Intersect`/`Except` |
| `Apache.DataFusion.Sample.Aggregate` | Read JSON and aggregate with `GROUP BY` |
| `Apache.DataFusion.Sample.ScalarUdf` | Register and call a scalar UDF |
| `Apache.DataFusion.Sample.TableProvider` | In-memory table via `SimpleTableProvider` |
| `Apache.DataFusion.Sample.Streaming` | Stream Arrow batches and read typed columns |
| `Apache.DataFusion.Sample.SessionConfig` | Configure the session builder + observability |
| `Apache.DataFusion.Sample.Excel` | Read an `.xlsx` spreadsheet and run a SQL query |

Run any of them with:

```powershell
dotnet run --project examples/Apache.DataFusion.Sample.Sql
```

## Feature surface

- **`SessionContext`**: SQL execution, configured builder (`CreateBuilder`), `GetOption`,
  memory usage, runtime stats (feature-gated), object-store registration, external table
  provider registration, and register/read for Parquet, CSV, JSON, Arrow IPC, Avro, and
  Excel/ODS spreadsheets (`RegisterExcel`/`ReadExcel`, via the native `calamine` parser).
- **`DataFrame`**: `Collect`, `ExecuteStream`, `Count`, `Show`, strong Arrow schema access,
  schema IPC, `Explain`/`Cache`/`Describe`, and projection/filter/limit/distinct/drop/
  rename/with-column/unnest transformations.
- **Set operations**: union, union distinct, union by name, intersect, except.
- **Joins**: column joins and SQL-predicate joins.
- **Repartition and sort**.
- **Writes**: Parquet, CSV, JSON.
- **Plan exchange**: DataFusion logical plan Protobuf, Substrait (feature-gated).
- **Table providers**: `SimpleTableProvider` backed by Arrow IPC and a native `MemTable`;
  PostgreSQL, MySQL, MongoDB, ClickHouse, and SQLite backed by
  [`datafusion-table-providers`](https://github.com/datafusion-contrib/datafusion-table-providers).
- **Scalar UDF**: zero-argument `Int64` managed callbacks.
- **Typed exceptions**: `DataFusionException` (base), plus `DataFusionPlanException`,
  `DataFusionExecutionException`, `DataFusionIoException`,
  `DataFusionResourcesExhaustedException`, `DataFusionConfigurationException`, and
  `DataFusionNotImplementedException`.

## Optional native features

Heavier integrations are gated behind Cargo features and **disabled by default** so the
base native library stays small. Calling one of these APIs without the matching feature
compiled in returns a clear native error message.

| Capability | Cargo feature(s) |
| --- | --- |
| Object stores | `object-store-aws`, `object-store-gcp`, `object-store-http` |
| PostgreSQL | `postgres` |
| MySQL | `mysql` |
| MongoDB | `mongodb` |
| ClickHouse | `clickhouse` |
| SQLite | `sqlite` |

Excel/ODS reading uses the lightweight `calamine` parser and is gated behind the `excel`
feature, which is **enabled by default**. Drop it with `--no-default-features` if you do not
need spreadsheet support.

### External table providers

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

Each `RegisterXxx` call above builds a connection pool, registers one table and
releases the pool. When registering several tables from the same database,
reuse a factory so the pool is built once:

```csharp
using SessionContext context = new();

// Build the PostgreSQL connection pool once...
using PostgresTableFactory factory = new(
    "host=localhost port=5432 dbname=postgres user=postgres password=password sslmode=disable");

// ...then register many tables against the same pool.
factory.Register(context, "companies", "companies", schemaName: "public");
factory.Register(context, "orders", "orders", schemaName: "public");
```

Pool creation and table registration are cooperatively cancellable. Pass a
`CancellationToken` to abort a slow connect or registration; a cancelled call
throws `OperationCanceledException`:

```csharp
using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
using PostgresTableFactory factory = new(connectionString, cts.Token);
factory.Register(context, "companies", "companies", cancellationToken: cts.Token);
```

`MySqlTableFactory`, `SqliteTableFactory`, `ClickHouseTableFactory` and
`MongoDbTableFactory` expose the same pattern.

## Native packaging

Native binaries are packaged through RID-specific projects under `packages/`, placing the
binary under `runtimes/{rid}/native/` to match .NET native asset conventions:

```powershell
dotnet pack packages/Apache.DataFusion.Native.win-x64.csproj -c Release
```

Supported RIDs: `win-x64`, `linux-x64`, `linux-arm64`, `linux-musl-x64`,
`linux-musl-arm64`, `osx-x64`, `osx-arm64`.

## Notes and limitations

`Collect` and `ExecuteStream` hand results to the C# side through the Arrow C Data Interface:
the native engine fully drives the query (honouring the optional `CancellationToken`) and then
shares the materialized record-batch buffers directly, so `ArrowBatchReader.Transport` reports
`CDataInterface` and no Arrow IPC serialize/deserialize round-trip is paid. The Arrow IPC path
is retained internally as a fallback (for example, `SimpleTableProvider` still consumes IPC
bytes).
