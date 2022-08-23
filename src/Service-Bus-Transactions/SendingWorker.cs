using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

namespace ServiceBus.TestApp
{
    public class SendingWorker : BackgroundService
    {
        private readonly ILogger<SendingWorker> _logger;
        private readonly IServiceProvider _serviceProvider;
        private readonly ServiceBusClient _serviceBusClient;
        private ServiceBusSender? _payloadSender;
        private readonly IConfiguration _configuration;
        public SendingWorker(ILogger<SendingWorker> logger, IServiceProvider serviceCollection, ServiceBusClient serviceBusClient,
             IConfiguration configuration)
        {
            _logger = logger;
            _serviceProvider = serviceCollection;
            _serviceBusClient = serviceBusClient;
            _configuration = configuration;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            Console.Title =
                $"Sending {payLoad.Length} size messages.  Transactions {_configuration.GetValue<bool>("EnableTransactions")}";

            //Wait for consumers before starting sender.
            await Task.Delay(5000, stoppingToken);

            await using (_payloadSender = _serviceBusClient.CreateSender("1"))
            {
                _logger.LogInformation("ReceivingWorker running at: {time}", DateTimeOffset.Now);

                while (!stoppingToken.IsCancellationRequested)
                {
                    await SendPayLoad(1000, stoppingToken);
                    await Task.Delay(800, stoppingToken);
                }
            }
        }

        private async Task SendPayLoad(int count, CancellationToken stoppingToken)
        {
            for (int x = 0; x < count; x+=5)
            {
                var list = new List<ServiceBusMessage>();

                for (int y = 0; y < 5; y++)
                {
                    var serviceBusMessage = new ServiceBusMessage(new BinaryData(payLoad));
                    list.Add(serviceBusMessage);
                }

                if (_payloadSender != null) await _payloadSender.SendMessagesAsync(list, stoppingToken);
                //await Task.Delay(1000, stoppingToken);
            }
        }

        private readonly string payLoad =
            "public class ReceivingWorker : BackgroundService\r\n    {\r\n        private readonly ILogger<ReceivingWorker> _logger;\r\n        private readonly IServiceProvider _serviceProvider;\r\n        private readonly ServiceBusClient _serviceBusClient;\r\n\r\n        public ReceivingWorker(ILogger<ReceivingWorker> logger, IServiceProvider serviceCollection, ServiceBusClient serviceBusClient)\r\n        {\r\n            _logger = logger;\r\n            _serviceProvider = serviceCollection;\r\n            _serviceBusClient = serviceBusClient;\r\n        }\r\n\r\n        protected override async Task ExecuteAsync(CancellationToken stoppingToken)\r\n        {\r\n            //Create 3 consumer that send to the next.  Final consumer deletes message.\r\n            var c1 = _serviceProvider.GetRequiredService<Consumer>();\r\n            var c2 = _serviceProvider.GetRequiredService<Consumer>();\r\n            var c3 = _serviceProvider.GetRequiredService<Consumer>();\r\n\r\n            await c1.Start(\"1\", \"2\", stoppingToken);\r\n            await c2.Start(\"2\", \"3\", stoppingToken);\r\n            await c3.Start(\"3\", \"\", stoppingToken);\r\n\r\n            var initialLoadSender = _serviceBusClient.CreateSender(\"1\");\r\n\r\n            for(int x=0; x< 10000; x++)\r\n            {\r\n                var serviceBusMessage = new ServiceBusMessage(new BinaryData(body)))\r\n                initialLoadSender.SendMessageAsync()\r\n            }\r\n\r\n            while (!stoppingToken.IsCancellationRequested)\r\n            {\r\n                _logger.LogInformation(\"ReceivingWorker running at: {time}\", DateTimeOffset.Now);\r\n                \r\n\r\n                await Task.Delay(1000, stoppingToken);\r\n            }\r\n        }\r\n        private string payLoad = \"\"";
    }

}