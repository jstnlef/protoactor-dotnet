using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Proto.Cluster.Identity;
using Proto.Cluster.Metrics;
using Proto.Extensions;
using Proto.Utils;

namespace Proto.Cluster;

public class NewClusterContext : IClusterContext
{
    private readonly ILogger _logger;
    private readonly Cluster _cluster;

    private readonly IIdentityLookup _identityLookup;
    private readonly PidCache _pidCache;
    private readonly ShouldThrottle _requestLogThrottle;
    private readonly ActorSystem _system;
    private readonly int _requestTimeoutSeconds;

    public NewClusterContext(Cluster cluster, ILogger? logger = null)
    {
        _logger = logger ?? NullLogger.Instance;
        _identityLookup = cluster.IdentityLookup;
        _pidCache = cluster.PidCache;
        var config = cluster.Config;
        _system = cluster.System;
        _cluster = cluster;

        _requestLogThrottle = Throttle.Create(
            config.MaxNumberOfEventsInRequestLogThrottlePeriod,
            config.RequestLogThrottlePeriod,
            i => _logger.LogInformation("Throttled {LogCount} TryRequestAsync logs", i)
        );

        _requestTimeoutSeconds = (int)config.ActorRequestTimeout.TotalSeconds;
    }

    public async Task<T?> RequestAsync<T>(
        ClusterIdentity clusterIdentity,
        object message,
        ISenderContext context,
        CancellationToken ct)
    {
        //for member requests, we need to wait for the cluster to be ready
        if (!_cluster.MemberList.IsClient)
        {
            if (!_cluster.JoinedCluster.IsCompletedSuccessfully)
            {
                await _cluster.JoinedCluster;
            }
        }

        var i = 0;

        var future = context.GetFuture();
        PID? lastPid = null;

        try
        {
            var lookupTimer = Stopwatch.StartNew();

            while (!ct.IsCancellationRequested && !context.System.Shutdown.IsCancellationRequested)
            {
                i++;

                if (i > 1 && _logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.LogDebug("RequestAsync attempt {Attempt} for {ClusterIdentity}", i, clusterIdentity);
                }

                var source = PidSource.Cache;
                var pid = clusterIdentity.CachedPid ?? (_pidCache.TryGet(clusterIdentity, out var tmp) ? tmp : null);

                if (pid is null)
                {
                    source = PidSource.Lookup;
                    pid = await GetPidFromLookup(clusterIdentity, context, ct).ConfigureAwait(false);
                }

                if (pid is null)
                {
                    if (_logger.IsEnabled(LogLevel.Debug))
                    {
                        _logger.LogDebug("Requesting {ClusterIdentity} - Did not get PID from IdentityLookup",
                            clusterIdentity);
                    }

                    if (lookupTimer.Elapsed.TotalSeconds > _requestTimeoutSeconds)
                    {
                        throw new TimeoutException("Request timed out");
                    }

                    await Task.Delay(i * 20, CancellationToken.None).ConfigureAwait(false);

                    continue;
                }

                // Ensures that a future is not re-used against another actor.
                // avoid equality check for perf
                // ReSharper disable once PossibleUnintendedReferenceComparison
                if (lastPid is not null && pid != lastPid)
                {
                    RefreshFuture();
                }

                Stopwatch t = null!;

                if (context.System.Metrics.Enabled)
                {
                    t = Stopwatch.StartNew();
                }

                try
                {
                    context.Request(pid, message, future.Pid);
                    var task = future.Task;

                    await task.WaitAsync(CancellationTokens.FromSeconds(_requestTimeoutSeconds)).ConfigureAwait(false);

                    if (task.IsCompleted)
                    {
                        var untypedResult = MessageEnvelope.UnwrapMessage(task.Result);

                        if (untypedResult is DeadLetterResponse)
                        {
                            if (!context.System.Shutdown.IsCancellationRequested && _logger.IsEnabled(LogLevel.Debug))
                            {
                                _logger.LogDebug("TryRequestAsync failed, dead PID from {Source}", source);
                            }

                            RefreshFuture();
                            await RemoveFromSource(clusterIdentity, PidSource.Lookup, pid).ConfigureAwait(false);

                            continue;
                        }

                        if (untypedResult is T t1)
                        {
                            return t1;
                        }

                        if (untypedResult == null) // timeout, actual valid response cannot be null
                        {
                            throw new TimeoutException("Request timed out");
                        }

                        if (typeof(T) == typeof(MessageEnvelope))
                        {
                            return (T)(object)MessageEnvelope.Wrap(task.Result);
                        }

                        _logger.LogError("Unexpected message. Was type {Type} but expected {ExpectedType}",
                            untypedResult.GetType(), typeof(T));

                        RefreshFuture();
                        await RemoveFromSource(clusterIdentity, source, pid).ConfigureAwait(false);

                        break;
                    }
                    else
                    {
                        if (!context.System.Shutdown.IsCancellationRequested)
                        {
                            if (_logger.IsEnabled(LogLevel.Debug))
                            {
                                _logger.LogDebug("TryRequestAsync timed out, PID from {Source}", source);
                            }
                        }

                        _pidCache.RemoveByVal(clusterIdentity, pid);
                    }
                }
                catch (TaskCanceledException)
                {
                    if (!context.System.Shutdown.IsCancellationRequested)
                    {
                        if (_logger.IsEnabled(LogLevel.Debug))
                        {
                            _logger.LogDebug("TryRequestAsync timed out, PID from {Source}", source);
                        }
                    }

                    _pidCache.RemoveByVal(clusterIdentity, pid);
                }
                catch (TimeoutException)
                {
                    lastPid = pid;
                    await RemoveFromSource(clusterIdentity, PidSource.Cache, pid).ConfigureAwait(false);

                    continue;
                }
                catch (Exception x)
                {
                    x.CheckFailFast();

                    if (!context.System.Shutdown.IsCancellationRequested && _requestLogThrottle().IsOpen())
                    {
                        if (_logger.IsEnabled(LogLevel.Debug))
                        {
                            _logger.LogDebug(x, "TryRequestAsync failed with exception, PID from {Source}", source);
                        }
                    }

                    _pidCache.RemoveByVal(clusterIdentity, pid);
                    RefreshFuture();
                    await RemoveFromSource(clusterIdentity, PidSource.Cache, pid).ConfigureAwait(false);
                    await Task.Delay(i * 20, CancellationToken.None).ConfigureAwait(false);

                    continue;
                }
                finally
                {
                    if (context.System.Metrics.Enabled)
                    {
                        var elapsed = t.Elapsed;

                        ClusterMetrics.ClusterRequestDuration
                            .Record(elapsed.TotalSeconds,
                                new KeyValuePair<string, object?>("id", _system.Id),
                                new KeyValuePair<string, object?>("address", _system.Address),
                                new KeyValuePair<string, object?>("clusterkind", clusterIdentity.Kind),
                                new KeyValuePair<string, object?>("messagetype", message.GetMessageTypeName()),
                                new KeyValuePair<string, object?>("pidsource",
                                    source == PidSource.Cache ? "PidCache" : "IIdentityLookup")
                            );
                    }
                }

                if (context.System.Metrics.Enabled)
                {
                    ClusterMetrics.ClusterRequestRetryCount.Add(
                        1, new KeyValuePair<string, object?>("id", _system.Id),
                        new KeyValuePair<string, object?>("address", _system.Address),
                        new KeyValuePair<string, object?>("clusterkind", clusterIdentity.Kind),
                        new KeyValuePair<string, object?>("messagetype", message.GetMessageTypeName())
                    );
                }
            }

            if (!context.System.Shutdown.IsCancellationRequested && _requestLogThrottle().IsOpen())
            {
                _logger.LogWarning("RequestAsync retried but failed for {ClusterIdentity}", clusterIdentity);
            }

            throw new TimeoutException("Request timed out");
        }
        finally
        {
            future.Dispose();
        }

        void RefreshFuture()
        {
            future.Dispose();
            future = context.GetFuture();
            lastPid = null;
        }
    }

    private async ValueTask RemoveFromSource(ClusterIdentity clusterIdentity, PidSource source, PID pid)
    {
        if (source == PidSource.Lookup)
        {
            await _identityLookup.RemovePidAsync(clusterIdentity, pid, CancellationToken.None).ConfigureAwait(false);
        }

        _pidCache.RemoveByVal(clusterIdentity, pid);
    }

    private async ValueTask<PID?> GetPidFromLookup(ClusterIdentity clusterIdentity, ISenderContext context,
        CancellationToken ct)
    {
        try
        {
            if (context.System.Metrics.Enabled)
            {
                var pid = await ClusterMetrics.ClusterResolvePidDuration
                    .Observe(
                        async () => await _identityLookup.GetAsync(clusterIdentity, ct).ConfigureAwait(false),
                        new KeyValuePair<string, object?>("id", _system.Id),
                        new KeyValuePair<string, object?>("address", _system.Address),
                        new KeyValuePair<string, object?>("clusterkind", clusterIdentity.Kind)
                    ).ConfigureAwait(false);

                if (pid is not null)
                {
                    _pidCache.TryAdd(clusterIdentity, pid);
                }

                return pid;
            }
            else
            {
                var pid = await _identityLookup.GetAsync(clusterIdentity, ct).ConfigureAwait(false);

                if (pid is not null)
                {
                    _pidCache.TryAdd(clusterIdentity, pid);
                }

                return pid;
            }
        }
        catch (Exception e) when (e is not IdentityIsBlockedException)
        {
            e.CheckFailFast();

            if (context.System.Shutdown.IsCancellationRequested)
            {
                return default;
            }

            if (_requestLogThrottle().IsOpen())
            {
                _logger.LogWarning(e, "Failed to get PID from IIdentityLookup for {ClusterIdentity}", clusterIdentity);
            }

            return null;
        }
    }

    private enum PidSource
    {
        Cache,
        Lookup
    }
}
