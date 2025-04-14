using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Platform.IOThub.Repository.POICache;
using Platform.IOThub.Repository.POICache.Models;
using Platform.IOTHub.Repository.POI.Repositories;
using Redis.OM;
using Redis.OM.Contracts;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Platform.IOTHub.ImportPOI.Service
{
    public class WorkerLoadMappingCatToRedis : BackgroundService
    {
        private readonly ILogger<WorkerLoadMappingCatToRedis> _logger;
        //private readonly ICategoryRepository _categoryRepository;
        private readonly PoiDbContext _poiDbContext;
        private readonly IServiceProvider _serviceProvider;

        public WorkerLoadMappingCatToRedis(ILogger<WorkerLoadMappingCatToRedis> logger
            //, ICategoryRepository categoryRepository
            , IServiceProvider serviceProvider
            , PoiDbContext poiDbContext)
        {
            _logger = logger;
            //_categoryRepository = categoryRepository;
            _serviceProvider = serviceProvider;
            _poiDbContext = poiDbContext;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            IRedisConnection _redis = _poiDbContext.RedisConnection;
            _redis.DropIndexAndAssociatedRecords(typeof(MappingVMCatCache));

            var indexInf = _redis.GetIndexInfo(typeof(MappingVMCatCache));
            if (indexInf == null)
            {
                _redis.CreateIndex(typeof(MappingVMCatCache));
            }

            using var scope = _serviceProvider.CreateScope();
            ICategoryRepository _categoryRepository = scope.ServiceProvider.GetRequiredService<ICategoryRepository>();
            var mappingCats = await _categoryRepository.GetAllMappingVMCat();
            foreach (var mappingCat in mappingCats)
            {
                var cachePoi = new IOThub.Repository.POICache.Models.MappingVMCatCache
                {
                    Id = mappingCat.Id,
                    MarkerSelectedUrl = mappingCat.MarkerSelectedUrl,
                    MarkerUrl = mappingCat.MarkerUrl,
                    ImageUrl = mappingCat.ImageUrl,
                    VmBrandId = mappingCat.VmBrandId ?? string.Empty,
                    VmCatId = mappingCat.VmCatId,
                    VmCatName = mappingCat.VmCatName,
                    VmlBrandCode = mappingCat.VmlBrandCode ?? string.Empty,
                    VmlCatCode = mappingCat.VmlCatCode,

                };

                if (!_poiDbContext.MappingVMCatCol.Any(x => x.Id == cachePoi.Id))
                {
                    await _poiDbContext.MappingVMCatCol.InsertAsync(cachePoi);
                }
                else
                {
                    await _poiDbContext.MappingVMCatCol.UpdateAsync(cachePoi);
                }

                _logger.LogInformation($"upsert {cachePoi.VmCatName}");
               
            }
            
            _logger.LogInformation("Done");
            Environment.Exit(0);
        }
    }
}
