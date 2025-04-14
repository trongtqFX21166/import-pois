using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Platform.IOTHub.ImportPOI.Service.Cache;
using Platform.IOTHub.Repository.POI.Entities;
using Platform.IOTHub.Repository.POI.Repositories;
using Platform.IOTHub.Repository.VMPOIRaw;
using Redis.OM;
using Redis.OM.Contracts;
using Redis.OM.Searching;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Platform.IOTHub.ImportPOI.Service
{
    public class WorkerLoadEvsePowerRedis : BackgroundService
    {
        private readonly ILogger<WorkerLoadEvsePowerRedis> _logger;
        private readonly IServiceProvider _serviceProvider;
        private readonly IConfiguration _configuration;

        private readonly IVMPoiRawRepository _vmPoiRawRepository;
        private readonly IVinfastStationRepo _vinfastStationRepo;


        public WorkerLoadEvsePowerRedis(ILogger<WorkerLoadEvsePowerRedis> logger
            , IServiceProvider serviceProvider
            , IConfiguration configuration
            , IVMPoiRawRepository vmPoiRawRepository
            , IVinfastStationRepo vinfastStationRepo)
        {
            _logger = logger;
            _serviceProvider = serviceProvider;
            _configuration = configuration;

            _vmPoiRawRepository = vmPoiRawRepository;
            _vinfastStationRepo = vinfastStationRepo;
        }


        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var config = GetConfigurationOptions();
            ConnectionMultiplexer.Connect(config);
            var provider = new RedisConnectionProvider(ConnectionMultiplexer.Connect(config));
            IRedisConnection _redis = provider.Connection;

            _redis.DropIndexAndAssociatedRecords(typeof(EvsePowerStatusCache));

            var indexInf = _redis.GetIndexInfo(typeof(EvsePowerStatusCache));
            if (indexInf == null)
            {
                _redis.CreateIndex(typeof(EvsePowerStatusCache));
            }
            IRedisCollection<EvsePowerStatusCache> _evesPowerCol = provider.RedisCollection<EvsePowerStatusCache>();

            using var scope = _serviceProvider.CreateScope();
            IPoiRepository poiRepository = scope.ServiceProvider.GetRequiredService<IPoiRepository>();

            int skip = 0;
            int pageSize = 500;
            while (true)
            {
                try
                {
                    var pois = await _vinfastStationRepo.QueryMappingVfStations(skip, pageSize);
                    if (pois == null || !pois.Any())
                    {
                        break;
                    }

                    foreach (var poi in pois)
                    {
                        try
                        {
                            var vfStation = await _vinfastStationRepo.GetVfStation(poi.LocationId);
                            if (vfStation == null)
                            {
                                _logger.LogError($"Not found locationid {poi.LocationId}");
                                continue;
                            }

                            if (vfStation.evsePowers.Count == 0)
                            {
                                _logger.LogError($"Not found evePowers locationid {poi.LocationId}");
                                continue;
                            }

                            for (int i = 0; i < vfStation.evsePowers.Count; ++i)
                            {
                                var vfPower = vfStation.evsePowers[i];

                                if (_evesPowerCol.Any(x => x.Id == $"{poi.LocationId}_{i + 1}"))
                                {
                                    await _evesPowerCol.UpdateAsync(new EvsePowerStatusCache
                                    {
                                        Id = $"{poi.LocationId}_{i + 1}",
                                        Type = vfPower.type,
                                        TotalEvse = vfPower.totalEvse,
                                        LocationId = poi.LocationId,
                                        ExpiredTime = DateTimeOffset.Now.AddMinutes(1).ToUnixTimeSeconds(),
                                        NumberOfAvailableEvse = vfPower.totalEvse,
                                        VMLId = poi.VMLId
                                    });
                                }
                                else
                                {
                                    await _evesPowerCol.InsertAsync(new EvsePowerStatusCache
                                    {
                                        Id = $"{poi.LocationId}_{i + 1}",
                                        Type = vfPower.type,
                                        TotalEvse = vfPower.totalEvse,
                                        LocationId = poi.LocationId,
                                        ExpiredTime = DateTimeOffset.Now.AddMinutes(1).ToUnixTimeSeconds(),
                                        NumberOfAvailableEvse = vfPower.totalEvse,
                                        VMLId = poi.VMLId
                                    });
                                }

                            }
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError($"Unhandel error with {poi.LocationId}:{ex.Message}");
                        }
                    }
                    _logger.LogInformation($"WorkerLoadEvsePowerRedis:skip:{skip}:{pageSize}");
                }
                catch (Exception ex) { 
                }
                skip++;
            }

            _logger.LogInformation("WorkerLoadEvsePowerRedis:Done");
            Environment.Exit(0);
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
