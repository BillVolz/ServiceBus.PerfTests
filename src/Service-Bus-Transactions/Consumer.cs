﻿using System.Diagnostics;
using System.Transactions;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Azure;

namespace ServiceBus.TestApp
{
    public class Consumer
    {
        private ServiceBusClient _serviceBusClient;
        private readonly ILogger<ServiceBusProcessor> _logger;
        private ServiceBusSender? _sender;
        private string? _queName;
        private const bool EnableTransactions = true;
        private readonly Random _random;
        private readonly Queue<TimeSpan> _commitTimes = new Queue<TimeSpan>();
        private readonly IAzureClientFactory<ServiceBusClient> _serviceBugClientFactory;
        private long messageProcessedCount = 0;

        public Consumer(ILogger<ServiceBusProcessor> logger, IAzureClientFactory<ServiceBusClient> serviceBugClientFactory)
        {
            _logger = logger;
            _serviceBugClientFactory = serviceBugClientFactory;
            _random = new Random();

        }

        public async Task Start(string source, string destination, CancellationToken cancellationToken)
        {
            _serviceBusClient = _serviceBugClientFactory.CreateClient(source);

            _queName = source;

            _sender = string.IsNullOrEmpty(destination) ? null :
                 _serviceBusClient.CreateSender(destination);

            // create the options to use for configuring the processor
            var options = new ServiceBusProcessorOptions
            {
                // By default or when AutoCompleteMessages is set to true, the processor will complete the message after executing the message handler
                // Set AutoCompleteMessages to false to [settle messages](https://docs.microsoft.com/en-us/azure/service-bus-messaging/message-transfers-locks-settlement#peeklock) on your own.
                // In both cases, if the message handler throws an exception without settling the message, the processor will abandon the message.
                AutoCompleteMessages = false,
                ReceiveMode = ServiceBusReceiveMode.PeekLock,
                // I can also allow for multi-threading
                MaxConcurrentCalls = 20,
                PrefetchCount = 50,
                //MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(10)
            };


            var processor = _serviceBusClient.CreateProcessor(_queName, options);
            processor.ProcessMessageAsync += Processor_ProcessMessageAsync;
            processor.ProcessErrorAsync += Processor_ProcessErrorAsync;

            await processor.StartProcessingAsync(cancellationToken);
        }

        private async Task Processor_ProcessMessageAsync(ProcessMessageEventArgs arg)
        {
            var body = arg.Message.Body.ToString();

            if (_logger.IsEnabled(LogLevel.Trace)) _logger.LogTrace(message: body);

            //Wait a random amount of time to simulate work.
            //await Task.Delay(_random.Next(5, 10), arg.CancellationToken);

            var stopWatch = Stopwatch.StartNew();

            var serviceBusMessage = new ServiceBusMessage(new BinaryData(body))
            {
                //Needed for standard tier partitioning.
                //TransactionPartitionKey = arg.Message.TransactionPartitionKey,
                //PartitionKey = arg.Message.PartitionKey
            };

            //If sender is not null pass on to next queue.
            if (_sender != null)
            {
                using (var ts = EnableTransactions
                           ? new TransactionScope(TransactionScopeAsyncFlowOption.Enabled)
                           : null)
                {
                    await arg.CompleteMessageAsync(arg.Message, arg.CancellationToken);

                    await _sender.SendMessageAsync(serviceBusMessage, arg.CancellationToken);
                    ts?.Complete();
                }
            }
            else
            {
                await arg.CompleteMessageAsync(arg.Message, arg.CancellationToken);
            }

            lock (this)
            {
                _commitTimes.Enqueue(stopWatch.Elapsed);
                if (_commitTimes.Count > 200)
                    _commitTimes.Dequeue();
            }

            Interlocked.Increment(ref messageProcessedCount);
        }

        public long GetMessagesProcessed()
        {
            return Interlocked.Exchange(ref messageProcessedCount, 0);
        }

        public TimeSpan GetMaxMessageTime()
        {
            long maxTicks = 0;

            if (_commitTimes.Count == 0)
                return new TimeSpan(0);

            lock (this)
            {
                
                maxTicks = _commitTimes.Max(x => x.Ticks);
            }

            return new TimeSpan(Convert.ToInt64(maxTicks));
        }
        public TimeSpan GetAverageMessageTime()
        {
            double averageTicks = 0;
            if (_commitTimes.Count == 0)
                return new TimeSpan(0);

            lock (this)
            {
                averageTicks = _commitTimes.Average(x => x.Ticks);
            }
            return new TimeSpan(Convert.ToInt64(averageTicks));
        }

        private Task Processor_ProcessErrorAsync(ProcessErrorEventArgs arg)
        {
            _logger.LogError(arg.Exception, "Processor_ProcessErrorAsync");
            return Task.CompletedTask;
        }
    }
}
