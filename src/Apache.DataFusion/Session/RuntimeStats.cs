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

namespace Apache.DataFusion;

public readonly record struct RuntimeStats(
    int NumWorkers,
    long LiveTasksCount,
    long GlobalQueueDepth,
    long ElapsedNanos,
    long TotalBusyNanos,
    ulong TotalParkCount,
    ulong TotalPollsCount,
    ulong TotalNoopCount,
    ulong TotalStealCount,
    ulong TotalLocalScheduleCount,
    ulong TotalOverflowCount)
{
    internal static RuntimeStats FromNative(long[] values)
    {
        if (values.Length != 11)
        {
            throw new DataFusionException($"Expected 11 runtime stats values, got {values.Length}.");
        }

        return new RuntimeStats(
            checked((int)values[0]),
            values[1],
            values[2],
            values[3],
            values[4],
            checked((ulong)values[5]),
            checked((ulong)values[6]),
            checked((ulong)values[7]),
            checked((ulong)values[8]),
            checked((ulong)values[9]),
            checked((ulong)values[10]));
    }
}
