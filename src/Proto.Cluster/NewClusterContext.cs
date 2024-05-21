using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
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
#pragma warning disable CS0618 // Type or member is obsolete
    private static readonly ILogger Logger = Log.CreateLogger<NewClusterContext>();
#pragma warning restore CS0618 // Type or member is obsolete

    private readonly Cluster _cluster;
    private readonly IIdentityLookup _identityLookup;
    private readonly PidCache _pidCache;
    private readonly ShouldThrottle _requestLogThrottle;
    private readonly ActorSystem _system;

    public NewClusterContext(Cluster cluster)
    {
        _identityLookup = cluster.IdentityLookup;
        _pidCache = cluster.PidCache;
        _system = cluster.System;
        _cluster = cluster;

        var config = cluster.Config;
        _requestLogThrottle = Throttle.Create(
            config.MaxNumberOfEventsInRequestLogThrottlePeriod,
            config.RequestLogThrottlePeriod,
            i => Logger.LogInformation("Throttled {LogCount} TryRequestAsync logs", i)
        );
    }

    public async Task<T?> RequestAsync<T>(
        ClusterIdentity clusterIdentity,
        object message,
        ISenderContext context,
        CancellationToken ct)
    {
        if (_cluster.MemberList.Stopping)
        {
            throw new InvalidOperationException("Cluster is shutting down");
        }

        //for member requests, we need to wait for the cluster to be ready
        if (!_cluster.MemberList.IsClient)
        {
            if (!_cluster.JoinedCluster.IsCompletedSuccessfully)
            {
                await _cluster.JoinedCluster;
            }
        }

        using var cancelTokenSource = CancellationTokenSource.CreateLinkedTokenSource(ct, context.System.Shutdown);
        var cancelToken = cancelTokenSource.Token;
        Stopwatch stopwatch = null!;
        if (context.System.Metrics.Enabled)
        {
            stopwatch = Stopwatch.StartNew();
        }

        try
        {
            while (!cancelToken.IsCancellationRequested)
            {
                var pidResult = await GetPidAsync(clusterIdentity, context, cancelToken).ConfigureAwait(false);
                var pid = pidResult.Pid;
                var source = pidResult.Source;

                try
                {
                    return await context.RequestAsync<T>(pid, message, cancelToken).ConfigureAwait(false);
                }
                catch (AddressIsUnreachableException)
                {
                    // Address is unreachable. Let's clear the PID cache and allow the request to try again.
                    if (Logger.IsEnabled(LogLevel.Debug) && _requestLogThrottle().IsOpen())
                    {
                        Logger.LogDebug("RequestAsync to {ClusterIdentity} failed, {Address} is unreachable. PID from {Source}", clusterIdentity, pid.Address, source);
                    }
                    await HandleDeadPid(clusterIdentity, pid);
                }
                catch (DeadLetterException)
                {
                    // Dead PID. We want to try and get a new PID and attempt the request again
                    if (Logger.IsEnabled(LogLevel.Debug) && _requestLogThrottle().IsOpen())
                    {
                        Logger.LogDebug(
                            "RequestAsync to {ClusterIdentity} failed. Dead PID from {Source}. Retrying",
                            clusterIdentity,
                            source);
                    }

                    await HandleDeadPid(clusterIdentity, pid);
                }
            }
        }
        catch (TaskCanceledException)
        {
            // Ignored. This will fall through to throw the default TimeoutException.
        }
        catch (Exception x)
        {
            x.CheckFailFast();

            if (_requestLogThrottle().IsOpen())
            {
                Logger.LogError(x, "RequestAsync to {ClusterIdentity} failed with exception", clusterIdentity);
            }

            throw;
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
                        new KeyValuePair<string, object?>("messagetype", message.GetMessageTypeName())
                    );
            }
        }

        if (_requestLogThrottle().IsOpen())
        {
            Logger.LogWarning("RequestAsync to {ClusterIdentity} timed out", clusterIdentity);
        }

        throw new TimeoutException($"RequestAsync to {clusterIdentity} timed out");
    }

    private async ValueTask<GetPidResult> GetPidAsync(
        ClusterIdentity clusterIdentity,
        ISystemContext context,
        CancellationToken ct)
    {
        var source = PidSource.Cache;
        var pid = clusterIdentity.CachedPid ?? (_pidCache.TryGet(clusterIdentity, out var tmp) ? tmp : null);

        if (pid is not null)
        {
            return new GetPidResult(pid, source);
        }

        source = PidSource.Lookup;

        var retry = 0;
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

                if (Logger.IsEnabled(LogLevel.Debug))
                {
                    Logger.LogDebug(
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

        throw new TimeoutException($"RequestAsync to {clusterIdentity} timed out while fetching PID");
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

            if (_requestLogThrottle().IsOpen())
            {
                Logger.LogWarning(e, "Failed to get PID from IIdentityLookup for {ClusterIdentity}", clusterIdentity);
            }

            return default;
        }
    }

    private async ValueTask HandleDeadPid(ClusterIdentity clusterIdentity, PID pid)
    {
        await _identityLookup.RemovePidAsync(clusterIdentity, pid, CancellationToken.None).ConfigureAwait(false);
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
