using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.Azure;

namespace ServiceBus.TestApp
{
    public class Program
    {
        public static void Main(string[] args)
        {
            IHost host = Host.CreateDefaultBuilder(args)
                .ConfigureServices((hostContext,services) =>
                {
                    services.AddHostedService<ReceivingWorker>();
                    services.AddAzureClients(builder =>
                    {
                        builder.AddServiceBusClient(hostContext.Configuration.GetConnectionString("ServiceBus"))
                            .ConfigureOptions(options => options.EnableCrossEntityTransactions = true)
                            .WithName("1");
                        builder.AddServiceBusClient(hostContext.Configuration.GetConnectionString("ServiceBus"))
                            .ConfigureOptions(options => options.EnableCrossEntityTransactions = true)
                            .WithName("2");
                        builder.AddServiceBusClient(hostContext.Configuration.GetConnectionString("ServiceBus"))
                            .ConfigureOptions(options => options.EnableCrossEntityTransactions = true)
                            .WithName("3");
                        builder.AddServiceBusClient(hostContext.Configuration.GetConnectionString("ServiceBus"))
                            .ConfigureOptions(options => options.EnableCrossEntityTransactions = true);
                    });
                    services.AddTransient<Consumer>();
                    services.AddHostedService<SendingWorker>();
                    services.AddSingleton(x => new ServiceBusAdministrationClient(hostContext.Configuration.GetConnectionString("ServiceBus")));
                })
                .Build();

            host.Run();
        }
    }
}