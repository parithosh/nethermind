// SPDX-FileCopyrightText: 2022 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using System.Threading.Tasks;
using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Crypto;
using Nethermind.Logging;

namespace Nethermind.TxPool
{
    public class TxPoolSender : ITxSender
    {
        private readonly ITxPool _txPool;
        private readonly ITxSealer _sealer;
        private readonly INonceManager _nonceManager;
        private readonly IEthereumEcdsa _ecdsa;
        private readonly ILogger _logger;

        public TxPoolSender(ITxPool txPool, ITxSealer sealer, INonceManager nonceManager, IEthereumEcdsa ecdsa, ILogger logger)
        {
            _txPool = txPool ?? throw new ArgumentNullException(nameof(txPool));
            _sealer = sealer ?? throw new ArgumentNullException(nameof(sealer));
            _nonceManager = nonceManager ?? throw new ArgumentNullException(nameof(nonceManager));
            _ecdsa = ecdsa ?? throw new ArgumentException(nameof(ecdsa));
            _logger = logger;
        }

        public ValueTask<(Keccak, AcceptTxResult?)> SendTransaction(Transaction tx, TxHandlingOptions txHandlingOptions)
        {
            bool manageNonce = (txHandlingOptions & TxHandlingOptions.ManagedNonce) == TxHandlingOptions.ManagedNonce;
            tx.SenderAddress ??= _ecdsa.RecoverAddress(tx);
            if (tx.SenderAddress is null)
                throw new ArgumentNullException(nameof(tx.SenderAddress));
            _logger.Info($"SendTransaction {tx.SenderAddress} nonce: {_nonceManager.GetAccounts().GetAccount(tx.SenderAddress).Nonce}");
            if (manageNonce)
            {
                tx.Nonce = _nonceManager.ReserveNonce(tx.SenderAddress);
                _logger.Info($"SendTransaction assigning {tx.SenderAddress} {tx.Nonce}");
                txHandlingOptions |= TxHandlingOptions.AllowReplacingSignature;
            }
            else
            {
                _logger.Info($"SendTransaction with nonce {tx.SenderAddress} {tx.Nonce}");
                _nonceManager.TxWithNonceReceived(tx.SenderAddress, tx.Nonce);
            }
            _sealer.Seal(tx, txHandlingOptions);
            AcceptTxResult result = _txPool.SubmitTx(tx, txHandlingOptions);
            _logger.Info($"SendTransaction result {tx.SenderAddress} {tx.Nonce} {result == AcceptTxResult.Accepted}");

            if (result == AcceptTxResult.Accepted)
            {
                _nonceManager.TxAccepted(tx.SenderAddress);
            }
            else
            {
                _nonceManager.TxRejected(tx.SenderAddress);
            }

            return new ValueTask<(Keccak, AcceptTxResult?)>((tx.Hash!, result)); // The sealer calculates the hash
        }
    }
}
