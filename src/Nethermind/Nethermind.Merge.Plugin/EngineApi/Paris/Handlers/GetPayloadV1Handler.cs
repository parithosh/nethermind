// SPDX-FileCopyrightText: 2022 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System.Threading.Tasks;
using Nethermind.Core;
using Nethermind.Core.Extensions;
using Nethermind.JsonRpc;
using Nethermind.Logging;
using Nethermind.Merge.Plugin.BlockProduction;
using Nethermind.Merge.Plugin.EngineApi.Paris.Data;

namespace Nethermind.Merge.Plugin.EngineApi.Paris.Handlers
{
    /// <summary>
    /// engine_getPayloadV1
    ///
    /// Given a 8 byte payload_id, it returns the most recent version of an execution payload that is available by the time of the call or responds with an error.
    ///
    /// <see href="https://github.com/ethereum/execution-apis/blob/main/src/engine/paris.md#engine_getpayloadv1"/>
    /// </summary>
    /// <remarks>
    /// This call must be responded immediately. An exception would be the case when no version of the payload is ready yet and in this case there might be a slight delay before the response is done.
    /// Execution client should create a payload with empty transaction set to be able to respond as soon as possible.
    /// If there were no prior engine_preparePayload call with the corresponding payload_id or the process of building a payload has been cancelled due to the timeout then execution client must respond with error message.
    /// Execution client may stop the building process with the corresponding payload_id value after serving this call.
    /// </remarks>
    public abstract class GetPayloadV1AbstractHandler<TGetPayloadResult> : IAsyncHandler<byte[], TGetPayloadResult?> where TGetPayloadResult : IGetPayloadResult, new()
    {
        private readonly IPayloadPreparationService _payloadPreparationService;
        private readonly ILogger _logger;

        protected GetPayloadV1AbstractHandler(IPayloadPreparationService payloadPreparationService, ILogManager logManager)
        {
            _payloadPreparationService = payloadPreparationService;
            _logger = logManager.GetClassLogger();
        }

        public async Task<ResultWrapper<TGetPayloadResult?>> HandleAsync(byte[] payloadId)
        {
            string payloadStr = payloadId.ToHexString(true);
            IBlockProductionContext? blockContext = await _payloadPreparationService.GetPayload(payloadStr);
            Block? block = blockContext?.CurrentBestBlock;

            if (block is null)
            {
                // The call MUST return -38001: Unknown payload error if the build process identified by the payloadId does not exist.
                if (_logger.IsWarn) _logger.Warn($"Block production for payload with id={payloadId.ToHexString()} failed - unknown payload.");
                return ResultWrapper<TGetPayloadResult?>.Fail("unknown payload", MergeErrorCodes.UnknownPayload);
            }

            if (_logger.IsInfo) _logger.Info($"GetPayloadV1 result: {block.Header.ToString(BlockHeader.Format.Full)}.");

            Metrics.GetPayloadRequests++;
            Metrics.NumberOfTransactionsInGetPayload = block.Transactions.Length;
            return ConstructResult(blockContext!);
        }

        protected virtual ResultWrapper<TGetPayloadResult?> ConstructResult(IBlockProductionContext blockContext) =>
            ResultWrapper<TGetPayloadResult?>.Success(new TGetPayloadResult { Block = blockContext });
    }

    public sealed class GetPayloadV1Handler : GetPayloadV1AbstractHandler<ExecutionPayloadV1>
    {
        public GetPayloadV1Handler(IPayloadPreparationService payloadPreparationService, ILogManager logManager)
            : base(payloadPreparationService, logManager)
        {
        }
    }
}
