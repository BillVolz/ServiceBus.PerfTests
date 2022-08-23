using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

namespace ServiceBus.TestApp
{
    public class ReceivingWorker : BackgroundService
    {
        private readonly ILogger<ReceivingWorker> _logger;
        private readonly IServiceProvider _serviceProvider;
        private readonly ServiceBusClient _serviceBusClient;
        private readonly ServiceBusAdministrationClient _serviceBusAdministrationClient;
        private readonly IConfiguration _configuration;

        public ReceivingWorker(ILogger<ReceivingWorker> logger, IServiceProvider serviceCollection, ServiceBusClient serviceBusClient,
            ServiceBusAdministrationClient serviceBusAdministrationClient, IConfiguration configuration)
        {
            _logger = logger;
            _serviceProvider = serviceCollection;
            _serviceBusClient = serviceBusClient;
            _serviceBusAdministrationClient = serviceBusAdministrationClient;
            _configuration = configuration;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            //Create 3 queues,  1, 2 and 3.
            await CreateQueue("1", stoppingToken);
            await CreateQueue("2", stoppingToken);
            await CreateQueue("3", stoppingToken);

            //Create 3 receivers that send to the next.  Final consumer deletes message, which is signified by the empty string.
            var c1 = _serviceProvider.GetRequiredService<Receiver>();
            var c2 = _serviceProvider.GetRequiredService<Receiver>();
            var c3 = _serviceProvider.GetRequiredService<Receiver>();

            //Start all 3 receivers
            await c1.Start("1", "2", stoppingToken);
            await c2.Start("2", "3", stoppingToken);
            await c3.Start("3", "", stoppingToken);
          
            _logger.LogInformation("ReceivingWorker running at: {time}", DateTimeOffset.Now);

            //Run this in the background to show performance data.
            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(1000, stoppingToken);
                _logger.LogWarning("Average Time / Message Count / Max Time\r\n{c1} / {b1} / {a1}\r\n{c2} / {b2} / {a2}\r\n{c3} / {b3} / {a3}\r\n",
                    c1.GetAverageMessageTime(), c1.GetMessagesProcessed(), c1.GetMaxMessageTime(),
                    c2.GetAverageMessageTime(), c2.GetMessagesProcessed(),c2.GetMaxMessageTime(),
                    c3?.GetAverageMessageTime(), c3?.GetMessagesProcessed(), c3?.GetMaxMessageTime());
            }
            
        }
        /// <summary>
        /// Creates the queue using the preferred options if the queue doesn't already exists.
        /// </summary>
        /// <param name="newQueueName"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        private async Task CreateQueue(string newQueueName, CancellationToken cancellationToken)
        {
            if (await _serviceBusAdministrationClient.QueueExistsAsync(newQueueName, cancellationToken))
                return;

            var queueOptions = new CreateQueueOptions(newQueueName)
            {
                MaxDeliveryCount = 200,
                LockDuration = TimeSpan.FromMinutes(2),
                EnablePartitioning = false,
                AutoDeleteOnIdle = TimeSpan.MaxValue,
                DeadLetteringOnMessageExpiration = true,
                MaxSizeInMegabytes =
                    5120, // Should be this but must be a bug since its giving the wrong size-> properties.MaxSizeInMegabytes
            };

            await _serviceBusAdministrationClient.CreateQueueAsync(queueOptions, cancellationToken);
        }
    }

}