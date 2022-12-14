// SPDX-FileCopyrightText: 2022 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using System.IO;
using Nethermind.Core.Crypto;
using Nethermind.Core.Extensions;
using Nethermind.Core.Test.Builders;
using Nethermind.Crypto;
using Nethermind.Int256;
using Nethermind.Logging;
using Nethermind.Serialization.Rlp;
using NUnit.Framework;

namespace Nethermind.Core.Test.Encoding;

[TestFixture]
public class BlockDecoderTests2
{


    [Test]
    public void Can_do_roundtrip_null([Values(true, false)] bool valueDecoder)
    {
        BlockDecoder decoder = new();
        Rlp result = decoder.Encode(null);
        Block decoded = valueDecoder ? Rlp.Decode<Block>(result.Bytes.AsSpan()) : Rlp.Decode<Block>(result);
        Assert.IsNull(decoded);
    }

    [Test]
    public void Can_do_roundtrip_blob()
    {
        var block =
            Build.A.Block
                .WithNumber(1)
                .WithTransactions(Build.A.Transaction.WithType(TxType.Blob).WithBlobVersionedHashes(new []{ new byte[32] }).TestObject)
                .WithUncles(Build.A.BlockHeader.TestObject)
                .WithWithdrawals(8)
                .WithExcessDataGas(0)
                .WithMixHash(Keccak.EmptyTreeHash)
                .WithTimestamp(HeaderDecoder.Eip4844TransitionTimestamp)
                .TestObject;
        BlockDecoder decoder = new();
        Rlp encoded = decoder.Encode(block);
        Block decoded = decoder.Decode(new RlpStream(encoded.Bytes));
    }
    [Test]
    public void Can_do_roundtrip_blob()
    {
        var block =
            Build.A.Block
                .WithNumber(1)
                .WithTransactions(Build.A.Transaction.WithType(TxType.Blob).WithBlobVersionedHashes(new []{ new byte[32] }).TestObject)
                .WithUncles(Build.A.BlockHeader.TestObject)
                .WithWithdrawals(8)
                .WithExcessDataGas(0)
                .WithMixHash(Keccak.EmptyTreeHash)
                .WithTimestamp(HeaderDecoder.Eip4844TransitionTimestamp)
                .TestObject;
        BlockDecoder decoder = new();
        Rlp encoded = decoder.Encode(block);
        Block decoded = decoder.Decode(new RlpStream(encoded.Bytes));
    }
}
