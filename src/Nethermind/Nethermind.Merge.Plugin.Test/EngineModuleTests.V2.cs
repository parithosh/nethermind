// SPDX-FileCopyrightText: 2022 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using FluentAssertions;
using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Core.Extensions;
using Nethermind.Core.Test.Builders;
using Nethermind.JsonRpc;
using Nethermind.JsonRpc.Test;
using Nethermind.Merge.Plugin.BlockProduction;
using Nethermind.Merge.Plugin.Data;
using NUnit.Framework;

namespace Nethermind.Merge.Plugin.Test;

public partial class EngineModuleTests
{
    [Test]
    public virtual async Task Should_process_block_as_expected_V2()
    {
        using MergeTestBlockchain chain = await CreateShanghaiBlockChain(new MergeConfig { TerminalTotalDifficulty = "0" });
        IEngineRpcModule rpc = CreateEngineModule(chain);
        Keccak startingHead = chain.BlockTree.HeadHash;
        Keccak prevRandao = Keccak.Zero;
        Address feeRecipient = TestItem.AddressC;
        ulong timestamp = Timestamper.UnixTime.Seconds;
        var fcuState = new
        {
            headBlockHash = startingHead.ToString(),
            safeBlockHash = startingHead.ToString(),
            finalizedBlockHash = Keccak.Zero.ToString()
        };
        Withdrawal[] withdrawals = new[]
        {
            new Withdrawal { Index = 1, Amount = 3, Address = TestItem.AddressB, ValidatorIndex = 2 }
        };
        var payloadAttrs = new
        {
            timestamp = timestamp.ToHexString(true),
            prevRandao = prevRandao.ToString(),
            suggestedFeeRecipient = feeRecipient.ToString(),
            withdrawals
        };
        string?[] @params = new string?[]
        {
            chain.JsonSerializer.Serialize(fcuState),
            chain.JsonSerializer.Serialize(payloadAttrs)
        };
        string expectedPayloadId = "0x6454408c425ddd96";

        string response = RpcTest.TestSerializedRequest(rpc, "engine_forkchoiceUpdatedV2", @params!);
        JsonRpcSuccessResponse? successResponse = chain.JsonSerializer.Deserialize<JsonRpcSuccessResponse>(response);

        successResponse.Should().NotBeNull();
        response.Should().Be(chain.JsonSerializer.Serialize(new JsonRpcSuccessResponse
        {
            Id = successResponse.Id,
            Result = new ForkchoiceUpdatedV1Result
            {
                PayloadId = expectedPayloadId,
                PayloadStatus = new PayloadStatusV1
                {
                    LatestValidHash = new("0x1c53bdbf457025f80c6971a9cf50986974eed02f0a9acaeeb49cafef10efd133"),
                    Status = PayloadStatus.Valid,
                    ValidationError = null
                }
            }
        }));

        Keccak blockHash = new Keccak("0x6817d4b48be0bc14f144cc242cdc47a5ccc40de34b9c3934acad45057369f576");
        ExecutionPayload expectedPayload = new ExecutionPayload
        {
            BaseFeePerGas = 0,
            BlockHash = blockHash,
            BlockNumber = 1,
            ExtraData = Bytes.FromHexString("0x4e65746865726d696e64"), // Nethermind
            FeeRecipient = feeRecipient,
            GasLimit = chain.BlockTree.Head!.GasLimit,
            GasUsed = 0,
            LogsBloom = Bloom.Empty,
            ParentHash = startingHead,
            PrevRandao = prevRandao,
            ReceiptsRoot = chain.BlockTree.Head!.ReceiptsRoot!,
            StateRoot = new("0xde9a4fd5deef7860dc840612c5e960c942b76a9b2e710504de9bab8289156491"),
            Timestamp = timestamp,
            Transactions = Array.Empty<byte[]>(),
            Withdrawals = withdrawals
        };

        response = RpcTest.TestSerializedRequest(rpc, "engine_getPayloadV2", expectedPayloadId);
        successResponse = chain.JsonSerializer.Deserialize<JsonRpcSuccessResponse>(response);

        successResponse.Should().NotBeNull();
        response.Should().Be(chain.JsonSerializer.Serialize(new JsonRpcSuccessResponse
        {
            Id = successResponse.Id,
            Result = expectedPayload
        }));

        response = RpcTest.TestSerializedRequest(rpc, "engine_newPayloadV2",
            chain.JsonSerializer.Serialize(expectedPayload));
        successResponse = chain.JsonSerializer.Deserialize<JsonRpcSuccessResponse>(response);

        successResponse.Should().NotBeNull();
        response.Should().Be(chain.JsonSerializer.Serialize(new JsonRpcSuccessResponse
        {
            Id = successResponse.Id,
            Result = new PayloadStatusV1
            {
                LatestValidHash = blockHash,
                Status = PayloadStatus.Valid,
                ValidationError = null
            }
        }));

        fcuState = new
        {
            headBlockHash = blockHash.ToString(true),
            safeBlockHash = blockHash.ToString(true),
            finalizedBlockHash = startingHead.ToString(true)
        };
        @params = new[]
        {
            chain.JsonSerializer.Serialize(fcuState),
            null
        };

        response = RpcTest.TestSerializedRequest(rpc, "engine_forkchoiceUpdatedV2", @params!);
        successResponse = chain.JsonSerializer.Deserialize<JsonRpcSuccessResponse>(response);

        successResponse.Should().NotBeNull();
        response.Should().Be(chain.JsonSerializer.Serialize(new JsonRpcSuccessResponse
        {
            Id = successResponse.Id,
            Result = new ForkchoiceUpdatedV1Result
            {
                PayloadId = null,
                PayloadStatus = new PayloadStatusV1
                {
                    LatestValidHash = blockHash,
                    Status = PayloadStatus.Valid,
                    ValidationError = null
                }
            }
        }));
    }

    [Test]
    public virtual async Task engine_forkchoiceUpdatedV1_should_fail_with_withdrawals()
    {
        using MergeTestBlockchain chain = await CreateBlockChain(new MergeConfig { TerminalTotalDifficulty = "0" });
        IEngineRpcModule rpcModule = CreateEngineModule(chain);
        var fcuState = new
        {
            headBlockHash = Keccak.Zero.ToString(),
            safeBlockHash = Keccak.Zero.ToString(),
            finalizedBlockHash = Keccak.Zero.ToString()
        };
        var payloadAttrs = new
        {
            timestamp = "0x0",
            prevRandao = Keccak.Zero.ToString(),
            suggestedFeeRecipient = Address.Zero.ToString(),
            withdrawals = Enumerable.Empty<Withdrawal>()
        };
        string[] @params = new[]
        {
            chain.JsonSerializer.Serialize(fcuState),
            chain.JsonSerializer.Serialize(payloadAttrs)
        };

        string response = RpcTest.TestSerializedRequest(rpcModule, "engine_forkchoiceUpdatedV1", @params);
        JsonRpcErrorResponse? errorResponse = chain.JsonSerializer.Deserialize<JsonRpcErrorResponse>(response);

        errorResponse.Should().NotBeNull();
        errorResponse!.Error.Should().NotBeNull();
        errorResponse!.Error!.Code.Should().Be(ErrorCodes.InvalidParams);
        errorResponse!.Error!.Message.Should().Contain("Withdrawals not supported");
    }

    [TestCaseSource(nameof(GetWithdrawalValidationValues))]
    public virtual async Task engine_forkchoiceUpdatedV2_should_validate_withdrawals((
        string CreateBlockchainMethod,
        string ErrorMessage,
        IEnumerable<Withdrawal>? Withdrawals
        ) input)
    {
        MethodInfo createBlockchain = typeof(EngineModuleTests).GetMethod(
            input.CreateBlockchainMethod,
            BindingFlags.Instance | BindingFlags.NonPublic,
            new[] { typeof(IMergeConfig), typeof(IPayloadPreparationService) })!;

        using MergeTestBlockchain chain = await (Task<MergeTestBlockchain>)
            createBlockchain.Invoke(this, new object?[] { new MergeConfig { TerminalTotalDifficulty = "0" }, null })!;

        IEngineRpcModule rpcModule = CreateEngineModule(chain);
        var fcuState = new
        {
            headBlockHash = chain.BlockTree.HeadHash.ToString(),
            safeBlockHash = chain.BlockTree.HeadHash.ToString(),
            finalizedBlockHash = Keccak.Zero.ToString()
        };
        var payloadAttrs = new
        {
            timestamp = Timestamper.UnixTime.Seconds.ToHexString(true),
            prevRandao = Keccak.Zero.ToString(),
            suggestedFeeRecipient = TestItem.AddressA.ToString(),
            withdrawals = input.Withdrawals
        };
        string[] @params = new[]
        {
            chain.JsonSerializer.Serialize(fcuState),
            chain.JsonSerializer.Serialize(payloadAttrs)
        };

        string response = RpcTest.TestSerializedRequest(rpcModule, "engine_forkchoiceUpdatedV2", @params);
        JsonRpcErrorResponse? errorResponse = chain.JsonSerializer.Deserialize<JsonRpcErrorResponse>(response);

        errorResponse.Should().NotBeNull();
        errorResponse!.Error.Should().NotBeNull();
        errorResponse!.Error!.Code.Should().Be(MergeErrorCodes.InvalidPayloadAttributes);
        errorResponse!.Error!.Message.Should().Be(string.Format(input.ErrorMessage, string.Empty));
    }

    [Test]
    public virtual async Task engine_newPayloadV1_should_fail_with_withdrawals()
    {
        using MergeTestBlockchain chain = await CreateBlockChain(new MergeConfig { TerminalTotalDifficulty = "0" });
        IEngineRpcModule rpcModule = CreateEngineModule(chain);
        ExecutionPayload expectedPayload = new ExecutionPayload
        {
            BaseFeePerGas = 0,
            BlockHash = Keccak.Zero,
            BlockNumber = 1,
            ExtraData = Array.Empty<byte>(),
            FeeRecipient = Address.Zero,
            GasLimit = 0,
            GasUsed = 0,
            LogsBloom = Bloom.Empty,
            ParentHash = Keccak.Zero,
            PrevRandao = Keccak.Zero,
            ReceiptsRoot = Keccak.Zero,
            StateRoot = Keccak.Zero,
            Timestamp = 0,
            Transactions = Array.Empty<byte[]>(),
            Withdrawals = Enumerable.Empty<Withdrawal>()
        };

        string response = RpcTest.TestSerializedRequest(rpcModule, "engine_newPayloadV1",
            chain.JsonSerializer.Serialize(expectedPayload));
        JsonRpcErrorResponse? errorResponse = chain.JsonSerializer.Deserialize<JsonRpcErrorResponse>(response);

        errorResponse.Should().NotBeNull();
        errorResponse!.Error.Should().NotBeNull();
        errorResponse!.Error!.Code.Should().Be(ErrorCodes.InvalidParams);
        errorResponse!.Error!.Message.Should().Contain("Withdrawals not supported");
    }

    [TestCaseSource(nameof(GetWithdrawalValidationValues))]
    public virtual async Task engine_newPayloadV2_should_validate_withdrawals((
        string CreateBlockchainMethod,
        string ErrorMessage,
        IEnumerable<Withdrawal>? Withdrawals
        ) input)
    {
        MethodInfo createBlockchain = typeof(EngineModuleTests).GetMethod(
            input.CreateBlockchainMethod,
            BindingFlags.Instance | BindingFlags.NonPublic,
            new[] { typeof(IMergeConfig), typeof(IPayloadPreparationService) })!;

        using MergeTestBlockchain chain = await (Task<MergeTestBlockchain>)
            createBlockchain.Invoke(this, new object?[] { new MergeConfig { TerminalTotalDifficulty = "0" }, null })!;

        IEngineRpcModule rpcModule = CreateEngineModule(chain);
        Keccak blockHash = new Keccak("0x6817d4b48be0bc14f144cc242cdc47a5ccc40de34b9c3934acad45057369f576");
        Keccak startingHead = chain.BlockTree.HeadHash;
        Keccak prevRandao = Keccak.Zero;
        Address feeRecipient = TestItem.AddressC;
        ulong timestamp = Timestamper.UnixTime.Seconds;
        ExecutionPayload expectedPayload = new ExecutionPayload
        {
            BaseFeePerGas = 0,
            BlockHash = blockHash,
            BlockNumber = 1,
            ExtraData = Bytes.FromHexString("0x4e65746865726d696e64"), // Nethermind
            FeeRecipient = feeRecipient,
            GasLimit = chain.BlockTree.Head!.GasLimit,
            GasUsed = 0,
            LogsBloom = Bloom.Empty,
            ParentHash = startingHead,
            PrevRandao = prevRandao,
            ReceiptsRoot = chain.BlockTree.Head!.ReceiptsRoot!,
            StateRoot = new("0xde9a4fd5deef7860dc840612c5e960c942b76a9b2e710504de9bab8289156491"),
            Timestamp = timestamp,
            Transactions = Array.Empty<byte[]>(),
            Withdrawals = input.Withdrawals
        };

        string response = RpcTest.TestSerializedRequest(rpcModule, "engine_newPayloadV2",
            chain.JsonSerializer.Serialize(expectedPayload));
        JsonRpcSuccessResponse? successResponse = chain.JsonSerializer.Deserialize<JsonRpcSuccessResponse>(response);

        successResponse.Should().NotBeNull();
        response.Should().Be(chain.JsonSerializer.Serialize(new JsonRpcSuccessResponse
        {
            Id = successResponse.Id,
            Result = new PayloadStatusV1
            {
                LatestValidHash = null,
                Status = PayloadStatus.Invalid,
                ValidationError = string.Format(input.ErrorMessage, $"in block {blockHash} ")
            }
        }));
    }

    protected static IEnumerable<(
    string CreateBlockchainMethod,
        string ErrorMessage,
        IEnumerable<Withdrawal>? Withdrawals
        )> GetWithdrawalValidationValues()
    {
        yield return (nameof(CreateShanghaiBlockChain), "Withdrawals cannot be null {0}when EIP-4895 activated.", null);
        yield return (nameof(CreateBlockChain), "Withdrawals must be null {0}when EIP-4895 not activated.", Enumerable.Empty<Withdrawal>());
    }
}
