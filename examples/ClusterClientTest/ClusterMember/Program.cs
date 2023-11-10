using echo.messages;
using Proto;
using Proto.Cluster;
using Proto.Cluster.Consul;
using Proto.Cluster.PartitionActivator;
using Proto.Cluster.PubSub;
using Proto.Remote;
using Proto.Remote.GrpcNet;
using Acknowledge = echo.messages.Acknowledge;

var system = new ActorSystem(new ActorSystemConfig().WithDeveloperSupervisionLogging(true))
  .WithRemote(GrpcNetRemoteConfig.BindToLocalhost(8090).WithProtoMessages(EchoReflection.Descriptor))
  .WithCluster(ClusterConfig
    .Setup("MyCluster",
      new ConsulProvider(new ConsulProviderConfig()),
      new PartitionActivatorLookup())
    .WithClusterKind("Echo", Props.FromProducer(_ => new EchoActor()))
  );

await system
  .Cluster()
  .StartMemberAsync();

Console.WriteLine("Started");

Console.WriteLine("Press enter to exit");
Console.ReadLine();
Console.WriteLine("Shutting Down...");
await system.Cluster().ShutdownAsync();


internal class EchoActor : IActor
{
  private PID _client;

  public async Task ReceiveAsync(IContext context)
  {
    switch (context.Message)
    {
      case ClientConnected m:
        _client = PID.FromAddress(m.Address, m.Id);
        context.Respond(new Acknowledge());

        while (true)
        {
          var response = await context.RequestAsync<Acknowledge>(
            _client,
            new Message { Body = "hello!" },
            CancellationTokens.FromSeconds(10));
          await Task.Delay(TimeSpan.FromSeconds(2));
        }

        break;
    }
  }
}
