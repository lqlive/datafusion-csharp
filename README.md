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
| `src/Apache.DataFusion.TableProviders.*/` | Managed database table providers |
| `native/` | Rust `cdylib` exposing the C ABI |
| `proto/` | Protobuf contracts shared by C# and Rust |
| `tests/Apache.DataFusion.Tests/` | xUnit test suite |
| `examples/Apache.DataFusion.Sample.*/` | Runnable example projects (one per scenario) |
| `packages/` | RID-specific native NuGet packaging projects |

## Prerequisites

- **.NET SDK 10.0.300+** &mdash; required to build the `.slnx` solution format
  (pinned to `10.0.300` in `global.json`). The core SDK targets `net8.0`;
  database table-provider packages target `net10.0`.
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

The default native build includes Excel/ODS reading. Extra native integrations are
opt-in via Cargo features (see [Optional native features](#optional-native-features)):

```powershell
cargo build --features "object-store-aws object-store-gcp object-store-http"
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
| `Apache.DataFusion.Sample.MySql` | Query a MySQL table through a managed streaming provider |
| `Apache.DataFusion.Sample.PostgreSql` | Query a PostgreSQL table through a managed streaming provider |
| `Apache.DataFusion.Sample.Sqlite` | Query a SQLite table through a managed streaming provider |
| `Apache.DataFusion.Sample.MongoDB` | Query a MongoDB collection through a managed streaming provider |
| `Apache.DataFusion.Sample.ClickHouse` | Query a ClickHouse table through the official `ClickHouse.Driver` |

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
  `StreamingTableProvider`, a lazy callback-based provider whose `Scan` is invoked by the
  engine and streamed in over the Arrow C Data Interface (connect any managed source, e.g.
  ADO.NET, with no native database driver). Managed provider packages currently include
  MySQL, PostgreSQL, SQLite, MongoDB, and ClickHouse.
- **Scalar UDF**: zero-argument `Int64` managed callbacks.
- **Typed exceptions**: `DataFusionException` (base), plus `DataFusionPlanException`,
  `DataFusionExecutionException`, `DataFusionIoException`,
  `DataFusionResourcesExhaustedException`, `DataFusionConfigurationException`, and
  `DataFusionNotImplementedException`.

## Optional native features

The default native build includes the spreadsheet reader. Some extra integrations are
gated behind Cargo features so the base native library does not pull in every
backend-specific dependency. Calling one of these APIs without the matching feature
compiled in returns a clear native error message.

| Capability | Cargo feature(s) |
| --- | --- |
| Object stores | `object-store-aws`, `object-store-gcp`, `object-store-http` |
| Substrait plan exchange | `substrait` |
| Runtime metrics | `runtime-metrics` |

Excel/ODS reading uses the lightweight `calamine` parser and is gated behind the `excel`
feature, which is enabled by default. Drop default features with `--no-default-features`
only when you intentionally want a smaller custom native build.

### Streaming table providers

External data sources (databases, web services, custom readers) are connected from the
managed side rather than via native drivers. Implement `StreamingTableProvider`, expose the
Arrow schema, and return a fresh `IArrowArrayStream` on each scan. DataFusion invokes `Scan`
lazily on every query and reads the batches over the Arrow C Data Interface, so no Arrow IPC
round-trip and no native database/TLS dependency are involved.

```csharp
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.DataFusion;

sealed class AdoNetTableProvider(Schema schema, string connectionString, string sql) : StreamingTableProvider
{
    public override Schema Schema { get; } = schema;

    // Called by the engine on every scan; open the reader lazily and adapt it
    // to an IArrowArrayStream (e.g. with your own ADO.NET -> Arrow bridge).
    public override IArrowArrayStream Scan() => OpenAdoNetArrowStream(connectionString, sql, Schema);
}

using SessionContext context = new();
context.RegisterStreamingTable("companies", new AdoNetTableProvider(schema, connectionString, "SELECT * FROM companies"));

using DataFrame df = context.Sql("SELECT * FROM companies WHERE region = 'EU'");
Console.WriteLine(df.Count());
```

`Schema` is read once at registration and must match every stream `Scan` returns. `Scan`
may run on native worker threads and more than once over the table's lifetime; each call
must return a new, independently consumable stream, which the engine disposes once read.

### Managed database providers

Managed database provider packages are built on `StreamingTableProvider`. They keep database
drivers on the C# side, register discovered tables and collections in the current
`SessionContext`, and push simple projection, filter, and limit requests down when possible.

| Provider package | Extension method | Driver |
| --- | --- | --- |
| `Apache.DataFusion.TableProviders.MySql` | `RegisterMySql` | `MySqlConnector` |
| `Apache.DataFusion.TableProviders.PostgreSql` | `RegisterPostgreSql` | `Npgsql` |
| `Apache.DataFusion.TableProviders.Sqlite` | `RegisterSqlite` | `Microsoft.Data.Sqlite` |
| `Apache.DataFusion.TableProviders.MongoDB` | `RegisterMongoDb` | `MongoDB.Driver` |
| `Apache.DataFusion.TableProviders.ClickHouse` | `RegisterClickHouse` | official `ClickHouse.Driver` |

```csharp
using Apache.DataFusion;
using Apache.DataFusion.TableProviders.PostgreSql;

using SessionContext context = new();
context.RegisterPostgreSql("Host=localhost;Username=postgres;Password=pass;Database=datafusion_sample");

using DataFrame df = context.Sql("""
    SELECT *
    FROM datafusion_orders
    LIMIT 10
    """);
df.Show();
```

SQL database providers register tables and views from the selected database in one call:

```csharp
using Apache.DataFusion;
using Apache.DataFusion.TableProviders.MySql;

using SessionContext context = new();
context.RegisterMySql("Server=localhost;User ID=root;Password=pass;Database=datafusion_sample");

using DataFrame df = context.Sql("""
    SELECT *
    FROM datafusion_orders
    LIMIT 10
    """);
df.Show();
```

SQLite uses the same registration shape for a local database file:

```csharp
using Apache.DataFusion;
using Apache.DataFusion.TableProviders.Sqlite;

using SessionContext context = new();
context.RegisterSqlite("Data Source=datafusion_sample.sqlite");

using DataFrame df = context.Sql("""
    SELECT *
    FROM datafusion_orders
    LIMIT 10
    """);
df.Show();
```

Database registration uses each discovered table or collection name as the DataFusion table
name by default. Single-table registration overloads are also available when you only want
to expose one table.

The sample projects use environment variables for connection strings:

| Sample | Environment variable |
| --- | --- |
| `Apache.DataFusion.Sample.MySql` | `DATAFUSION_MYSQL_CONNECTION` |
| `Apache.DataFusion.Sample.PostgreSql` | `DATAFUSION_POSTGRESQL_CONNECTION` |
| `Apache.DataFusion.Sample.Sqlite` | `DATAFUSION_SQLITE_CONNECTION` |
| `Apache.DataFusion.Sample.MongoDB` | `DATAFUSION_MONGODB_CONNECTION` |
| `Apache.DataFusion.Sample.ClickHouse` | `DATAFUSION_CLICKHOUSE_CONNECTION` |

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
