/*
 * Copyright (c) 2018 Demerzel Solutions Limited
 * This file is part of the Nethermind library.
 *
 * The Nethermind library is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * The Nethermind library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with the Nethermind. If not, see <http://www.gnu.org/licenses/>.
 */

using System;
using System.Numerics;

namespace Nethermind.Evm.Precompiles
{
    public class EcMulPrecompiledContract : IPrecompiledContract
    {
        public static IPrecompiledContract Instance = new EcMulPrecompiledContract();

        private EcMulPrecompiledContract()
        {
        }

        public BigInteger Address => 7;

        public long BaseGasCost()
        {
            throw new NotImplementedException();
        }

        public long DataGasCost(byte[] inputData)
        {
            throw new NotImplementedException();
        }

        public byte[] Run(byte[] inputData)
        {
            throw new NotImplementedException();
        }
    }
}