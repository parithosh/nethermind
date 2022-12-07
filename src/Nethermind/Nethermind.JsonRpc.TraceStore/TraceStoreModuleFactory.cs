// SPDX-FileCopyrightText: 2022 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using Nethermind.Blockchain.Find;
using Nethermind.Blockchain.Receipts;
using Nethermind.Db;
using Nethermind.Evm.Tracing.ParityStyle;
using Nethermind.JsonRpc.Modules;
using Nethermind.JsonRpc.Modules.Trace;
using Nethermind.Logging;

namespace Nethermind.JsonRpc.TraceStore;

public class TraceStoreModuleFactory : ModuleFactoryBase<ITraceRpcModule>
{
    private readonly IRpcModuleFactory<ITraceRpcModule> _innerFactory;
    private readonly IDbWithSpan _traceStore;
    private readonly IBlockFinder _blockFinder;
    private readonly IReceiptFinder _receiptFinder;
    private readonly ITraceSerializer<ParityLikeTxTrace> _traceSerializer;
    private readonly ITraceSerializer<ParityTxTraceFromStore> _traceStoreSerializer;
    private readonly ILogManager _logManager;
    private readonly TraceStoreMode _mode;

    public TraceStoreModuleFactory(IRpcModuleFactory<ITraceRpcModule> innerFactory,
        IDbWithSpan traceStore,
        IBlockFinder blockFinder,
        IReceiptFinder receiptFinder,
        ITraceSerializer<ParityLikeTxTrace> traceSerializer,
        ITraceSerializer<ParityTxTraceFromStore> traceStoreSerializer,
        ILogManager logManager,
        TraceStoreMode mode)
    {
        _innerFactory = innerFactory;
        _traceStore = traceStore;
        _blockFinder = blockFinder;
        _receiptFinder = receiptFinder;
        _traceSerializer = traceSerializer;
        _traceStoreSerializer = traceStoreSerializer;
        _logManager = logManager;
        _mode = mode;
    }

    public override ITraceRpcModule Create() => new TraceStoreRpcModule(_innerFactory.Create(), _traceStore, _blockFinder, _receiptFinder, _traceSerializer, _traceStoreSerializer, _logManager, _mode);
}
