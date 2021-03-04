using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQAPI.RequestResponseLog;

namespace RabbitMqConsumerWorker
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IConfiguration _config;
        private IConnection _connection;
        private IModel _channel;

        public Worker(ILogger<Worker> logger, IConfiguration config)
        {
            _logger = logger;
            _config = config;
            InitRabbitMQ();
        }
        private void InitRabbitMQ()
        {
            var factory = new ConnectionFactory()
            {
                HostName = _config["RabbitMQ:HostName"],
                UserName = _config["RabbitMQ:UserName"],
                Password = _config["RabbitMQ:Password"],
            };
            factory.AutomaticRecoveryEnabled = true;
            //return factory.CreateConnection(_config["RabbitMQ:QueueName"]);
            _connection = factory.CreateConnection();
           
            _channel = _connection.CreateModel();
            _channel.QueueDeclare(queue: _config["RabbitMQ:QueueName"],
                            durable: true,
                            exclusive: false,
                            autoDelete: false,
                            arguments: null);
            _channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
        }
        public override void Dispose()
        {
            _channel.Close();
            _connection.Close();
            base.Dispose();
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
                var consumer = new EventingBasicConsumer(_channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body;
                    var message = JsonConvert.DeserializeObject<RequestResponse>(Encoding.UTF8.GetString(body));
                    Console.WriteLine("message received {0}", message);
                    _logger.LogInformation($"Message Received before try: {message}");

                    try
                    {
                        _logger.LogInformation($"Response from  Consumer: {message}");
                        Console.WriteLine("message received inside try{0}",message);
                        _channel.BasicAck(ea.DeliveryTag, false);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogInformation($" Consumer Exception: {ex}");
                        _channel.BasicReject(ea.DeliveryTag, true);
                    }
                };
                await Task.Delay(10);
                _channel.BasicConsume(queue: _config["RabbitMQ:QueueName"],
                                    autoAck: false,
                                    consumer: consumer);


                await Task.Delay(1000, stoppingToken);
            }
        }
    }
}
