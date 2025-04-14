using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Platform.IOThub.Repository.POICache;
using Platform.IOThub.Repository.POICache.Models;
using Platform.IOTHub.Repository.POI.Repositories;
using Redis.OM;
using Redis.OM.Contracts;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static System.Formats.Asn1.AsnWriter;

namespace Platform.IOTHub.ImportPOI.Service
{
    public class WorkerLoadPartyMappingToRedis : BackgroundService
    {
        
        private readonly ILogger<WorkerLoadPartyMappingToRedis> _logger;
        private readonly PoiDbContext _poiDbContext;
        private readonly IServiceProvider _serviceProvider;

        public WorkerLoadPartyMappingToRedis(ILogger<WorkerLoadPartyMappingToRedis> logger
            , PoiDbContext poiDbContext
            , IServiceProvider serviceProvider)
        {
            _logger = logger;
            _poiDbContext = poiDbContext;

            _serviceProvider = serviceProvider;
        }

        private IPartyMappingRepository _partyMappingRepository;
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
     
            IRedisConnection _redis = _poiDbContext.RedisConnection;
            _redis.DropIndexAndAssociatedRecords(typeof(PartyMappingCache));

            var indexInf = _redis.GetIndexInfo(typeof(PartyMappingCache));
            if (indexInf == null)
            {
                _redis.CreateIndex(typeof(PartyMappingCache));
            }

            List<string> sources = new List<string>()
            { 
                "Google",
                "Waze",
                "VM"
            };

            List<Task> tasks = new List<Task>();
            foreach (var source in sources) { 
                tasks.Add(loadRedis(stoppingToken, source));
            }

            await Task.WhenAll(tasks);

            _logger.LogInformation("Done");
        }

        private async Task loadRedis(CancellationToken stoppingToken, string source) {
            using var scope = _serviceProvider.CreateScope();
            IPartyMappingRepository _partyMappingRepository = scope.ServiceProvider.GetRequiredService<IPartyMappingRepository>();


            int page = 0;
            while (!stoppingToken.IsCancellationRequested) 
            {
                try
                {
                    var parties = await _partyMappingRepository.Query(source, page, 500);
                    if (parties == null
                        || !parties.Any()) {
                        break;
                    }

                    foreach (var party in parties) {
                        await _poiDbContext.PartyMappingCol.InsertAsync(new PartyMappingCache
                        {
                            PartyId = party.PartyId,
                            SourceId = party.SourceId,
                        });
                    }

                    page++;

                    _logger.LogInformation($"Loeaded source {source} from {page} and {page*500}");

                }
                catch (Exception ex) {
                    _logger.LogError(ex, ex.Message);
                }
            }

        }
    }
}
