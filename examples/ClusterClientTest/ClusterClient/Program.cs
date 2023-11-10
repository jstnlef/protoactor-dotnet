using echo.messages;
using Proto;
using Proto.Cluster;
using Proto.Cluster.Consul;
using Proto.Cluster.PartitionActivator;
using Proto.Cluster.PubSub;
using Proto.Remote;
using Proto.Remote.GrpcNet;
using Acknowledge = echo.messages.Acknowledge;

var system = new ActorSystem()
  .WithRemote(GrpcNetRemoteConfig.BindToLocalhost().WithProtoMessages(EchoReflection.Descriptor))
  .WithCluster(ClusterConfig
    .Setup("MyCluster",
      new ConsulProvider(new ConsulProviderConfig()),
      new PartitionActivatorLookup()));

await system
  .Cluster()
  .StartClientAsync();

Console.WriteLine("Started");

system.Root.Spawn(Props.FromFunc(async context =>
{
  switch (context.Message)
  {
    case Started:
      var response = await system.Cluster().RequestAsync<Acknowledge>(
        "actor",
        "Echo",
        new ClientConnected { Address = context.Self.Address, Id = context.Self.Id },
        CancellationTokens.FromSeconds(10)
      );
      break;
    case Message m:
      Console.WriteLine(m.Body);
      context.Respond(new Acknowledge());
      break;
  }
}));

Console.WriteLine("Press enter to exit");
Console.ReadLine();
Console.WriteLine("Shutting Down...");
await system.Cluster().ShutdownAsync();
