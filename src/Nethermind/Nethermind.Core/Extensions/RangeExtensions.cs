//  Copyright (c) 2021 Demerzel Solutions Limited
//  This file is part of the Nethermind library.
// 
//  The Nethermind library is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  The Nethermind library is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with the Nethermind. If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Diagnostics;
using Nethermind.Core.Crypto;

namespace Nethermind.Core.Extensions
{
    public static class RangeExtensions
    {
        public static bool Includes(this Range @this, int value)
            => value >= @this.Start.Value && value <= @this.End.Value;

        public static bool Includes(this Range @this, int value, int len)
        {
            var (offset, length) = @this.GetOffsetAndLength(len);
            return value >= offset && value < length + offset;
        }
        public static bool Intersects(this Range @this, Range other)
            => @this.Start.Value <= other.End.Value && other.Start.Value <= @this.End.Value;

    }
}
