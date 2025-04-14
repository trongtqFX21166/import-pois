using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Platform.IOTHub.Repository.POI.Repositories;
using Redis.OM.Contracts;
using Redis.OM;
using StackExchange.Redis;
using Microsoft.Extensions.Configuration;
using Redis.OM.Searching;
using Platform.IOTHub.ImportPOI.Service.Cache;
using Platform.IOTHub.ImportPOI.Service.Helper;
using Amazon.Runtime;

namespace Platform.IOTHub.ImportPOI.Service
{
    public class WorkerClearRedisService : BackgroundService
    {
        private readonly ILogger<WorkerLoadRedisService> _logger;
        private readonly IServiceProvider _serviceProvider;
        private readonly IConfiguration _configuration;


        public WorkerClearRedisService(ILogger<WorkerLoadRedisService> logger
            , IServiceProvider serviceProvider
            , IConfiguration configuration)
        {
            _logger = logger;
            _serviceProvider = serviceProvider;
            _configuration = configuration;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var config = GetConfigurationOptions();
            ConnectionMultiplexer.Connect(config);
            var provider = new RedisConnectionProvider(ConnectionMultiplexer.Connect(config));
            IRedisConnection _redis = provider.Connection;

            _redis.DropIndexAndAssociatedRecords(typeof(PoiSearchCache));
           
            _logger.LogInformation("Already drop index");
        }


        private ConfigurationOptions GetConfigurationOptions()
        {
            var configurationOptions = new ConfigurationOptions
            {
                SyncTimeout = 60000, // 60 seconds
                ConnectTimeout = 60000, // 60 seconds
                Password = _configuration["PoiRedis:Password"]
            };

            var hosts = (_configuration["PoiRedis:Hosts"] ?? string.Empty).Split(new[] { ';', ',', '|' }, StringSplitOptions.RemoveEmptyEntries);
            var ports = (_configuration["PoiRedis:Ports"] ?? string.Empty).Split(new[] { ';', ',', '|' }, StringSplitOptions.RemoveEmptyEntries);
            for (var i = 0; i < hosts.Count(); i++)
            {
                configurationOptions.EndPoints.Add(hosts[i], Convert.ToInt32(ports[i]));
            }

            return configurationOptions;
        }

    }
}
