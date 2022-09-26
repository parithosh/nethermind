using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.AspNetCore.DataProtection.Repositories;
using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Core.Specs;
using Nethermind.Evm.TransactionProcessing;
using Nethermind.Int256;
using Nethermind.State;

namespace Nethermind.Evm.Tracing
{
    public class GasEstimator
    {
        private readonly ITransactionProcessor _transactionProcessor;
        private readonly IReadOnlyStateProvider _stateProvider;
        private readonly ISpecProvider _specProvider;

        public GasEstimator(ITransactionProcessor transactionProcessor, IReadOnlyStateProvider stateProvider, ISpecProvider specProvider)
        {
            _transactionProcessor = transactionProcessor;
            _stateProvider = stateProvider;
            _specProvider = specProvider;
        }
        public long Estimate(Transaction tx, BlockHeader header, EstimateGasTracer gasTracer)
        {
            long lowBound = GasCostOf.Transaction - 1, highBound, cap;
            UInt256 feeCap;

            tx.SenderAddress ??= Address.Zero;

            highBound = tx.GasLimit != 0 && tx.GasLimit >= GasCostOf.Transaction
                ? gasTracer.GasSpent
                : header.GasLimit;

            if (tx.GasLimit != 0 && (tx.MaxFeePerGas != 0 || tx.MaxPriorityFeePerGas != 0))
            {
                return 0; // both gasPrice and (maxFeePerGas or maxPriorityFeePerGas) specified
            }
            else if (tx.GasPrice != 0)
            {
                feeCap = tx.GasPrice;
            }
            else if (tx.MaxFeePerGas != 0)
            {
                feeCap = tx.MaxFeePerGas;
            }
            else
            {
                feeCap = 0;
            }

            if (feeCap != 0)
            {
                UInt256 balance = _stateProvider.GetBalance(tx.SenderAddress);
                UInt256 available = balance;
                if (tx.Value != 0)
                {
                    if (tx.Value >= available)
                    {
                        return 0;
                    }
                    available -= tx.Value;
                }
                available.Divide(in feeCap, out UInt256 allowance);

                if (highBound > allowance && allowance.IsUint64)
                {
                    highBound = (long)allowance;
                }
            }

            long gasCap = tx.GasLimit;
            if (gasCap != 0 && highBound > gasCap)
            {
                highBound = gasCap;
            }
            cap = highBound;

            highBound = BinarySearchEstimate(lowBound, highBound, highBound, tx, header);

            if (highBound == cap)
            {
                if (!TryExecutableTransaction(tx, header, highBound))
                {
                    return 0;
                }
            }
            return highBound;
        }
        private long BinarySearchEstimate(long leftBound, long rightBound, long cap, Transaction tx, BlockHeader header)
        {
            while (leftBound + 1 < rightBound)
            {
                long mid = (leftBound + rightBound) / 2;
                if (!TryExecutableTransaction(tx, header, mid))
                {
                    leftBound = mid;
                }
                else
                {
                    rightBound = mid;
                }
            }

            if (rightBound == cap && !TryExecutableTransaction(tx, header, rightBound))
            {
                return 0;
            }

            return rightBound;
        }

        private bool TryExecutableTransaction(Transaction transaction, BlockHeader block, long gasLimit)
        {
            OutOfGasTracer tracer = new();
            transaction.GasLimit = (long)gasLimit;
            _transactionProcessor.CallAndRestore(transaction, block, tracer);

            return !tracer.OutOfGas;
        }

        private class OutOfGasTracer : ITxTracer
        {
            public OutOfGasTracer()
            {
                OutOfGas = false;
            }
            public bool IsTracingReceipt => true;
            public bool IsTracingActions => false;
            public bool IsTracingOpLevelStorage => false;
            public bool IsTracingMemory => false;
            public bool IsTracingInstructions => true;
            public bool IsTracingRefunds => false;
            public bool IsTracingCode => false;
            public bool IsTracingStack => false;
            public bool IsTracingState => false;
            public bool IsTracingStorage => false;
            public bool IsTracingBlockHash => false;
            public bool IsTracingAccess => false;

            public bool OutOfGas { get; set; }

            public byte[] ReturnValue { get; set; }

            public byte StatusCode { get; set; }

            public void MarkAsSuccess(Address recipient, long gasSpent, byte[] output, LogEntry[] logs, Keccak stateRoot = null)
            {
                ReturnValue = output;
                StatusCode = Evm.StatusCode.Success;
            }

            public void MarkAsFailed(Address recipient, long gasSpent, byte[] output, string error, Keccak stateRoot = null)
            {
                ReturnValue = output ?? Array.Empty<byte>();
                StatusCode = Evm.StatusCode.Failure;
            }

            public void StartOperation(int depth, long gas, Instruction opcode, int pc, bool isPostMerge = false)
            {
            }

            public void ReportOperationError(EvmExceptionType error)
            {
                OutOfGas |= error == EvmExceptionType.OutOfGas;
            }

            public void ReportOperationRemainingGas(long gas)
            {
            }

            public void SetOperationMemorySize(ulong newSize)
            {
            }

            public void ReportMemoryChange(long offset, in ReadOnlySpan<byte> data)
            {
            }

            public void ReportStorageChange(in ReadOnlySpan<byte> key, in ReadOnlySpan<byte> value)
            {
            }

            public void SetOperationStorage(Address address, UInt256 storageIndex, ReadOnlySpan<byte> newValue, ReadOnlySpan<byte> currentValue)
            {
            }

            public void LoadOperationStorage(Address address, UInt256 storageIndex, ReadOnlySpan<byte> value)
            {
            }

            public void ReportSelfDestruct(Address address, UInt256 balance, Address refundAddress)
            {
                throw new NotSupportedException();
            }

            public void ReportBalanceChange(Address address, UInt256? before, UInt256? after)
            {
                throw new NotSupportedException();
            }

            public void ReportCodeChange(Address address, byte[] before, byte[] after)
            {
                throw new NotSupportedException();
            }

            public void ReportNonceChange(Address address, UInt256? before, UInt256? after)
            {
                throw new NotSupportedException();
            }

            public void ReportAccountRead(Address address)
            {
            }

            public void ReportStorageChange(StorageCell storageCell, byte[] before, byte[] after)
            {
                throw new NotSupportedException();
            }

            public void ReportStorageRead(StorageCell storageCell)
            {
                throw new NotSupportedException();
            }

            public void ReportAction(long gas, UInt256 value, Address @from, Address to, ReadOnlyMemory<byte> input, ExecutionType callType, bool isPrecompileCall = false)
            {
                throw new NotSupportedException();
            }

            public void ReportActionEnd(long gas, ReadOnlyMemory<byte> output)
            {
                throw new NotSupportedException();
            }

            public void ReportActionError(EvmExceptionType exceptionType)
            {
                throw new NotSupportedException();
            }

            public void ReportActionEnd(long gas, Address deploymentAddress, ReadOnlyMemory<byte> deployedCode)
            {
                throw new NotSupportedException();
            }

            public void ReportBlockHash(Keccak blockHash)
            {
                throw new NotSupportedException();
            }

            public void ReportByteCode(byte[] byteCode)
            {
                throw new NotSupportedException();
            }

            public void ReportGasUpdateForVmTrace(long refund, long gasAvailable)
            {
            }

            public void ReportRefund(long refund)
            {
                throw new NotSupportedException();
            }

            public void ReportExtraGasPressure(long extraGasPressure)
            {
                throw new NotSupportedException();
            }

            public void ReportAccess(IReadOnlySet<Address> accessedAddresses, IReadOnlySet<StorageCell> accessedStorageCells)
            {
                throw new NotSupportedException();
            }

            public void SetOperationStack(List<string> stackTrace)
            {
            }

            public void ReportStackPush(in ReadOnlySpan<byte> stackItem)
            {
            }

            public void SetOperationMemory(List<string> memoryTrace)
            {
            }

        }
    }
}
