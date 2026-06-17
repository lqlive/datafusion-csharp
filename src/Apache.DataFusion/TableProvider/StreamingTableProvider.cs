// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using Apache.Arrow;
using Apache.Arrow.Ipc;

namespace Apache.DataFusion;

/// <summary>
/// A lazy, streaming table provider implemented in managed code. Register an
/// instance with <see cref="M:Apache.DataFusion.SessionContext.RegisterStreamingTable(System.String,Apache.DataFusion.StreamingTableProvider)"/>; DataFusion
/// then calls <see cref="Scan(StreamingTableScanRequest)"/> on every scan and reads the returned stream
/// over the Arrow C Data Interface, so rows are pulled on demand straight from
/// the managed source (for example an ADO.NET reader) without a native database
/// driver.
/// </summary>
/// <remarks>
/// <para><see cref="Schema"/> is read once at registration and must match the
/// schema of every stream returned by <see cref="Scan(StreamingTableScanRequest)"/>.</para>
/// <para><see cref="Scan(StreamingTableScanRequest)"/> may be invoked from arbitrary native worker threads
/// and more than once over the table's lifetime. Each call must return a new,
/// independently consumable <see cref="IArrowArrayStream"/>; the returned stream
/// is owned and disposed by the engine once it is fully read.</para>
/// </remarks>
public abstract class StreamingTableProvider
{
    /// <summary>The Arrow schema of the rows produced by this provider.</summary>
    public abstract Schema Schema { get; }

    /// <summary>
    /// Indicates whether this provider applies <see cref="StreamingTableScanRequest"/>
    /// projection, filter, and limit information itself.
    /// </summary>
    public virtual bool SupportsPushdown => false;

    /// <summary>
    /// Produce a fresh stream of record batches for a single scan. Implementations
    /// should open their underlying source lazily here so each scan reads current
    /// data.
    /// </summary>
    public abstract IArrowArrayStream Scan();

    /// <summary>
    /// Produce a fresh stream while applying scan pushdown information supplied
    /// by DataFusion. Providers that can translate projection, filter, or limit
    /// into their source query should override this method.
    /// </summary>
    public virtual IArrowArrayStream Scan(StreamingTableScanRequest request) => Scan();
}
