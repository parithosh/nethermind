﻿/*
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
using System.Net;
using System.Threading.Tasks;
using DotNetty.Buffers;
using DotNetty.Codecs;
using DotNetty.Handlers.Logging;
using DotNetty.Transport.Bootstrapping;
using DotNetty.Transport.Channels;
using DotNetty.Transport.Channels.Sockets;
using Nethermind.Blockchain;
using Nethermind.Core.Logging;
using Nethermind.Core.Model;
using Nethermind.Network.P2P;
using Nethermind.Network.Rlpx.Handshake;
using Nethermind.Network.Stats;

namespace Nethermind.Network.Rlpx
{
    public class RlpxPeer : IRlpxPeer
    {
        private IChannel _bootstrapChannel;
        private IEventLoopGroup _bossGroup;
        private IEventLoopGroup _workerGroup;

        private bool _isInitialized;
        internal readonly NodeId LocalNodeId;
        private readonly int _localPort;
        private readonly IEncryptionHandshakeService _encryptionHandshakeService;
        private readonly INodeStatsProvider _nodeStatsProvider;
        private readonly IMessageSerializationService _serializationService;
        private readonly ISynchronizationManager _synchronizationManager;
        private readonly ILogManager _logManager;
        private readonly ILogger _logger;

        public RlpxPeer(NodeId localNodeId, int localPort, ISynchronizationManager synchronizationManager, IMessageSerializationService messageSerializationService, IEncryptionHandshakeService encryptionHandshakeService, INodeStatsProvider nodeStatsProvider, ILogManager logManager)
        {
            _encryptionHandshakeService = encryptionHandshakeService ?? throw new ArgumentNullException(nameof(encryptionHandshakeService));
            _nodeStatsProvider = nodeStatsProvider ?? throw new ArgumentNullException(nameof(nodeStatsProvider));
            _logManager = logManager ?? throw new ArgumentNullException(nameof(logManager));
            _logger = logManager.GetClassLogger();
            _serializationService = messageSerializationService ?? throw new ArgumentNullException(nameof(messageSerializationService));
            _synchronizationManager = synchronizationManager ?? throw new ArgumentNullException(nameof(synchronizationManager));
            
            LocalNodeId =  localNodeId ?? throw new ArgumentNullException(nameof(localNodeId));
            _localPort = localPort;
        }
        
        public async Task Init()
        {
            if (_isInitialized)
            {
                throw new InvalidOperationException($"{nameof(PeerManager)} already initialized.");
            }

            _isInitialized = true;

            try
            {
                _bossGroup = new MultithreadEventLoopGroup(1);
                _workerGroup = new MultithreadEventLoopGroup();

                ServerBootstrap bootstrap = new ServerBootstrap();
                bootstrap
                    .Group(_bossGroup, _workerGroup)
                    .Channel<TcpServerSocketChannel>()
                    .ChildOption(ChannelOption.SoBacklog, 100)
                    .Handler(new LoggingHandler("BOSS", DotNetty.Handlers.Logging.LogLevel.TRACE))
                    .ChildHandler(new ActionChannelInitializer<ISocketChannel>(ch =>
                        InitializeChannel(ch, ConnectionDirection.Out, null, null,
                            ((IPEndPoint) ch.RemoteAddress).Address.ToString(), ((IPEndPoint) ch.RemoteAddress).Port)));

                _bootstrapChannel = await bootstrap.BindAsync(_localPort).ContinueWith(t =>
                {
                    if (t.IsFaulted)
                    {
                        _logger.Error($"{nameof(Init)} failed", t.Exception);
                        return null;
                    }

                    return t.Result;
                });

                if (_bootstrapChannel == null)
                {
                    throw new NetworkingException($"Failed to initialize {nameof(_bootstrapChannel)}", NetwokExceptionType.Other);
                }
            }
            catch (Exception ex)
            {
                _logger.Error($"{nameof(Init)} failed.", ex);
                // TODO: check what happens on nulls
                await Task.WhenAll(_bossGroup?.ShutdownGracefullyAsync(), _workerGroup?.ShutdownGracefullyAsync());
                throw;
            }
        }

        public async Task ConnectAsync(NodeId remoteId, string host, int port, INodeStats nodeStats)
        {
            if (_logger.IsTrace) _logger.Trace($"Connecting to {remoteId}@{host}:{port}");

            Bootstrap clientBootstrap = new Bootstrap();
            clientBootstrap.Group(_workerGroup);
            clientBootstrap.Channel<TcpSocketChannel>();

            clientBootstrap.Option(ChannelOption.TcpNodelay, true);
            clientBootstrap.Option(ChannelOption.MessageSizeEstimator, DefaultMessageSizeEstimator.Default);
            clientBootstrap.Option(ChannelOption.ConnectTimeout, Timeouts.InitialConnection);
            clientBootstrap.RemoteAddress(host, port);

            var connStatus = new ConnStatus {Timeout = false};

            clientBootstrap.Handler(new ActionChannelInitializer<ISocketChannel>(ch => InitializeChannel(ch, ConnectionDirection.Out, connStatus, remoteId, host, port, nodeStats)));

            var connectTask = clientBootstrap.ConnectAsync(new IPEndPoint(IPAddress.Parse(host), port));
            var firstTask = await Task.WhenAny(connectTask, Task.Delay(Timeouts.InitialConnection.Add(TimeSpan.FromSeconds(5))));
            if (firstTask != connectTask)
            {
                connStatus.Timeout = true;
                if (_logger.IsTrace) _logger.Trace($"Connection timed out: {remoteId}@{host}:{port}");
                throw new NetworkingException($"Failed to connect to {remoteId} (timeout)", NetwokExceptionType.Timeout);
            }

            if (connectTask.IsFaulted)
            {
                connStatus.Timeout = true;
                if (_logger.IsTrace)
                {
                    _logger.Trace($"Error when connecting to {remoteId}@{host}:{port}, error: {connectTask.Exception}");
                }

                throw new NetworkingException($"Failed to connect to {remoteId}", NetwokExceptionType.TargetUnreachable,connectTask.Exception);
            }

            if (_logger.IsTrace) _logger.Trace($"Connected to {remoteId}@{host}:{port}");
        }

        public event EventHandler<SessionEventArgs> SessionCreated; 
        
        private void InitializeChannel(IChannel channel, ConnectionDirection connectionDirection, ConnStatus connStatus, NodeId remoteId = null, string remoteHost = null, int? remotePort = null, INodeStats nodeStats = null)
        {   
            P2PSession session = new P2PSession(
                LocalNodeId,
                remoteId,
                _localPort,
                connectionDirection,
                _serializationService,
                _synchronizationManager,
                _nodeStatsProvider,
                nodeStats,
                _logManager);

            if (connectionDirection == ConnectionDirection.Out)
            {
                if (_logger.IsTrace)
                {
                    _logger.Trace($"Initializing {connectionDirection.ToString().ToUpper()} channel{(connectionDirection == ConnectionDirection.Out ? $": {remoteId}@{remoteHost}:{remotePort}" : string.Empty)}");
                }

                // this is the first moment we get confirmed publicKey of remote node in case of outgoing connections
                session.RemoteNodeId = remoteId;
                session.RemoteHost = remoteHost;
                session.RemotePort = remotePort;
            }
            
            SessionCreated?.Invoke(this, new SessionEventArgs(session));

            HandshakeRole role = connectionDirection == ConnectionDirection.In ? HandshakeRole.Recipient : HandshakeRole.Initiator;
            var handshakeHandler = new NettyHandshakeHandler(_encryptionHandshakeService, session, role, remoteId, _logManager, connStatus);
            
            IChannelPipeline pipeline = channel.Pipeline;
            pipeline.AddLast(new LoggingHandler(connectionDirection.ToString().ToUpper(), DotNetty.Handlers.Logging.LogLevel.TRACE));
            pipeline.AddLast("enc-handshake-dec", new LengthFieldBasedFrameDecoder(ByteOrder.BigEndian, ushort.MaxValue, 0, 2, 0, 0, true));
            pipeline.AddLast("enc-handshake-handler", handshakeHandler);

            channel.CloseCompletion.ContinueWith(async x =>
            {
                if (_logger.IsTrace)
                {
                    _logger.Trace($"Channel disconnected: {session.RemoteNodeId}");
                }

                await session.DisconnectAsync(DisconnectReason.ClientQuitting, DisconnectType.Remote);
            });
        }
        
        public async Task Shutdown()
        {
//            InternalLoggerFactory.DefaultFactory.AddProvider(new ConsoleLoggerProvider((s, level) => true, false));

            await _bootstrapChannel.CloseAsync().ContinueWith(t =>
            {
                if (t.IsFaulted)
                {
                    _logger.Error($"{nameof(Shutdown)} failed", t.Exception);
                }
            });

            await Task.WhenAll(_bossGroup.ShutdownGracefullyAsync(), _workerGroup.ShutdownGracefullyAsync())
                .ContinueWith(t =>
                {
                    if (t.IsFaulted)
                    {
                        _logger.Error($"Groups shutdown failed in {nameof(PeerManager)}", t.Exception);
                    }
                });
        }
    }

    public class ConnStatus
    {
        public bool Timeout { get; set; }
    }
}