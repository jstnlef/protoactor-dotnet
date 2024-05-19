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

/// <summary>
/// ClusterContext which will not retry requests to cluster actors unless the actor is going to be moved
/// because of a topology change.
/// </summary>
public class NewClusterContext : IClusterContext
{
    private readonly ILogger _logger;
    private readonly Cluster _cluster;

    private readonly IIdentityLookup _identityLookup;
    private readonly PidCache _pidCache;
    private readonly ShouldThrottle _requestLogThrottle;
    private readonly ActorSystem _system;

    public NewClusterContext(Cluster cluster, ILogger? logger = null)
    {
        _logger = logger ?? NullLogger.Instance;
        _identityLookup = cluster.IdentityLookup;
        _pidCache = cluster.PidCache;
        _system = cluster.System;
        _cluster = cluster;

        var config = cluster.Config;
        _requestLogThrottle = Throttle.Create(
            config.MaxNumberOfEventsInRequestLogThrottlePeriod,
            config.RequestLogThrottlePeriod,
            i => _logger.LogInformation("Throttled {LogCount} TryRequestAsync logs", i)
        );
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

        var cancelTokenSource = CancellationTokenSource.CreateLinkedTokenSource(ct, context.System.Shutdown);
        var cancelToken = cancelTokenSource.Token;

        while (!cancelToken.IsCancellationRequested)
        {
            var pidResult = await GetPidAsync(clusterIdentity, context, cancelToken).ConfigureAwait(false);
            var pid = pidResult.Pid;
            var source = pidResult.Source;

            Stopwatch stopwatch = null!;

            if (context.System.Metrics.Enabled)
            {
                stopwatch = Stopwatch.StartNew();
            }

            try
            {
                return await context.RequestAsync<T>(pid, message, cancelToken);
            }
            catch (DeadLetterException)
            {
                // Dead PID. We want to try and get a new PID and attempt the request again.
                if (!context.System.Shutdown.IsCancellationRequested && _logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.LogDebug("RequestAsync failed, dead PID from {Source}", source);
                }
                await RemoveFromSource(clusterIdentity, PidSource.Lookup, pid).ConfigureAwait(false);
            }
            catch (InvalidOperationException e)
            {
                // We got a bad value from the request. Timeout the request by breaking early.
                _logger.LogError(e, "Invalid Operation");
                await RemoveFromSource(clusterIdentity, source, pid).ConfigureAwait(false);
                break;
            }
            catch (TaskCanceledException)
            {
                if (!context.System.Shutdown.IsCancellationRequested && _logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.LogDebug("RequestAsync timed out, PID from {Source}", source);
                }
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
                await RemoveFromSource(clusterIdentity, PidSource.Cache, pid).ConfigureAwait(false);
                break;
            }
            finally
            {
                if (context.System.Metrics.Enabled)
                {
                    var elapsed = stopwatch.Elapsed;

                    ClusterMetrics.ClusterRequestDuration
                        .Record(elapsed.TotalSeconds,
                            new KeyValuePair<string, object?>("id", _system.Id),
                            new KeyValuePair<string, object?>("address", _system.Address),
                            new KeyValuePair<string, object?>("clusterkind", clusterIdentity.Kind),
                            new KeyValuePair<string, object?>("messagetype", message.GetMessageTypeName()),
                            new KeyValuePair<string, object?>("pidsource",
                                pidResult.Source == PidSource.Cache ? "PidCache" : "IIdentityLookup")
                        );
                }
            }
        }

        if (!context.System.Shutdown.IsCancellationRequested && _requestLogThrottle().IsOpen())
        {
            _logger.LogWarning("RequestAsync failed for {ClusterIdentity}", clusterIdentity);
        }

        throw new TimeoutException("Request timed out");
    }

    private async ValueTask<GetPidResult> GetPidAsync(ClusterIdentity clusterIdentity, ISystemContext context, CancellationToken ct)
    {
        var source = PidSource.Cache;
        var pid = clusterIdentity.CachedPid ?? (_pidCache.TryGet(clusterIdentity, out var tmp) ? tmp : null);

        if (pid is not null)
        {
            return new GetPidResult(pid, source);
        }

        var retry = 0;
        source = PidSource.Lookup;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                retry++;
                pid = await GetPidFromLookup(clusterIdentity, context, ct).ConfigureAwait(false);

                if (pid is not null)
                {
                    return new GetPidResult(pid, source);
                }

                if (_logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.LogDebug(
                        "Requesting {ClusterIdentity} - Did not get PID from IdentityLookup",
                        clusterIdentity);
                }

                await Task.Delay(retry * 20, ct).ConfigureAwait(false);
            }
            catch (TaskCanceledException)
            {
                break;
            }
        }
        throw new TimeoutException("Request timed out fetching PID");
    }

    private async ValueTask<PID?> GetPidFromLookup(
        ClusterIdentity clusterIdentity,
        ISystemContext context,
        CancellationToken ct)
    {
        try
        {
            PID? pid;
            if (context.System.Metrics.Enabled)
            {
                pid = await ClusterMetrics.ClusterResolvePidDuration
                    .Observe(
                        async () => await _identityLookup.GetAsync(clusterIdentity, ct).ConfigureAwait(false),
                        new KeyValuePair<string, object?>("id", _system.Id),
                        new KeyValuePair<string, object?>("address", _system.Address),
                        new KeyValuePair<string, object?>("clusterkind", clusterIdentity.Kind)
                    ).ConfigureAwait(false);
            }
            else
            {
                pid = await _identityLookup.GetAsync(clusterIdentity, ct).ConfigureAwait(false);
            }

            if (pid is not null)
            {
                _pidCache.TryAdd(clusterIdentity, pid);
            }

            return pid;
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

            return default;
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

    private struct GetPidResult
    {
        public PID Pid { get; }
        public PidSource Source { get; }
        public GetPidResult(PID pid, PidSource source)
        {
            Pid = pid;
            Source = source;
        }
    }

    private enum PidSource
    {
        Cache,
        Lookup
    }
}
