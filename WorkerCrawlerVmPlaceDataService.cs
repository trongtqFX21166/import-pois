using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Platform.IOTHub.ImportPOI.Service.Models.PeakApi;
using Platform.IOTHub.Repository.POI.Repositories;
using Platform.IOTHub.Repository.VMPOIRaw;
using Platform.IOTHub.Repository.VMPOIRaw.Entities;
using Platform.IOTHub.Utility;
using Platform.KafkaClient;
using System.Collections.Concurrent;
using System.Text;

namespace Platform.IOTHub.ImportPOI.Service
{
    public class WorkerCrawlerVmPlaceDataService : BackgroundService
    {
        private readonly IGoogleRawRepo _googleRawRepo;
        private readonly IVMPoiRawRepository _vmPoiRawRepository;
        private readonly IServiceProvider _serviceProvider;

        private readonly ILogger<WorkerCrawlerVmPlaceDataService> _logger;
        private readonly HttpClient _peakClient;
        private readonly IConsumer _consumer;

        public WorkerCrawlerVmPlaceDataService(
            IGoogleRawRepo googleRawRepo
            , IVMPoiRawRepository vmPoiRawRepository
            , IServiceProvider serviceProvider
            , ILogger<WorkerCrawlerVmPlaceDataService> logger
            , IHttpClientFactory httpClientFactory
            , IConsumer consumer)
        {
            _googleRawRepo = googleRawRepo;
            _vmPoiRawRepository = vmPoiRawRepository;

            _serviceProvider = serviceProvider;
            _logger = logger;

            _peakClient = httpClientFactory.CreateClient("PeakApi");

            //todo: register consume result
            _consumer = consumer;
            _consumer.Consume += _consumer_Consume;

        }

        private static ConcurrentDictionary<string, PeakApiEventData> QueueRunnIds = new ConcurrentDictionary<string, PeakApiEventData>();
        private void _consumer_Consume(Confluent.Kafka.ConsumeResult<Confluent.Kafka.Ignore, string> consumeResult)
        {
            _logger.LogDebug($"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");

            if (string.IsNullOrWhiteSpace(consumeResult?.Message?.Value))
            {
                _logger.LogError($"Income message is null.");
                return;
            }

            var msg = JsonConvert.DeserializeObject<PeakApiEventMsg>(consumeResult.Message.Value);

            if (msg != null
                && msg.eventType == "ACTOR.RUN.SUCCEEDED"
                && msg.eventData != null
                && !string.IsNullOrEmpty(msg.eventData.actorTaskId))
            {
                if (QueueRunnIds.ContainsKey(msg.eventData.actorTaskId))
                {
                    QueueRunnIds[msg.eventData.actorTaskId] = msg.eventData;
                }
            }
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var t = Task.Run(() =>
            {
                _consumer.RegisterConsume(stoppingToken);

            });
            using var scope = _serviceProvider.CreateScope();
            IPoiRepository _poiRepository = scope.ServiceProvider.GetRequiredService<IPoiRepository>();

            //  await manualImportRunId(_poiRepository,"ADN2TvV4nuvIz8eJ9");


            //1. get crawler version
            int versionCode = int.Parse(DateTime.Now.ToString("yyyyMMdd"));

            //2. If tasks are running, wait for procced before start new
            //await manualImportRunId("bFCfRUNPCdtzidekr", versionCode);

            //3. get total placeIds with new versionCode
            int pageSize = 500;
            //string run_id = "yEMl8of12fCQoeWSn";
            var total = await _googleRawRepo.GetTotalInvalidMapping();
            var pageCount = total / pageSize;

            //4. get all placeIds with new version Code
            List<string> _allSearchText = new List<string>();
            for (int skip = 0; skip < pageCount; skip++)
            {
                _logger.LogInformation($"reupdate-search-text {skip} {pageSize} total {total} pagecount {pageCount}");
                var _datas = await _googleRawRepo.GetInvalidMappings(skip, pageSize);
                if (_datas.Any())
                {
                    await UpdateMappingManual(_datas
                        .Where(x => !string.IsNullOrWhiteSpace(x.SearchText))
                        .Select(x => x.SearchText).ToList());
                    _allSearchText.AddRange(_datas
                        .Where(x => !string.IsNullOrWhiteSpace(x.SearchText))
                        .Select(x => x.SearchText));
                }
                
            }

            _allSearchText = _allSearchText.OrderBy(x => x).Distinct().ToList();
            List<string> searchTexts = new List<string>();
            for (int skip = 0; skip < pageCount; skip++)
            {
                //5. send craw placids
                searchTexts = _allSearchText.Skip(skip * pageSize).Take(pageSize).ToList();
                var existedRaws = await _googleRawRepo.GetCrawlerGooglesBySearchText(searchTexts.ToArray());

                if (existedRaws?.Any() ?? false)
                {
                    List<string> existedTexts = existedRaws.Select(x => x.searchString).ToList();
                    searchTexts = searchTexts.Except(existedTexts).ToList();
                }

                if (searchTexts.Count() < (pageSize - 200))
                {
                    continue;
                }
                

                RunInfo runInfo = await createTaskAndStartRun(searchTexts);
                if (runInfo == null
                    || string.IsNullOrEmpty(runInfo.run_id))
                {
                    await Task.Delay(1000);
                    _logger.LogInformation($"Cannot start start run skip {skip} pagesize {pageSize} count {searchTexts.Count}");
                    continue;
                }

                _logger.LogInformation($"Started start run skip {skip} pagesize {pageSize}");
                QueueRunnIds.TryAdd(runInfo.task_id, new PeakApiEventData());

                //6. wait and update version code
                await WaitAndProccessImport(_poiRepository, runInfo, versionCode, searchTexts);

                searchTexts = new List<string>();
            }


            _logger.LogInformation("done");
        }

        private async Task WaitAndProccessImport(IPoiRepository poiRepository, RunInfo _runInfo, int versionCode, List<string> searchTexts)
        {
            int count = 0;
            while (true)
            {
                count++;
                if (count > 90)
                {
                    break;
                }

                try
                {
                    if (!QueueRunnIds.TryGetValue(_runInfo.task_id, out var envetData))
                    {
                        break;
                    }

                    var _taskRunnings = await getTaskRunning();
                    if (_taskRunnings == null || _taskRunnings.runs == null)
                    {
                        await Task.Delay(1000 * 60); // 1 m
                        continue;
                    }

                    if (!_taskRunnings.runs.Any(x => x.task_id == _runInfo.task_id
                    && x.status == "SUCCEEDED"))
                    {
                        await Task.Delay(1000 * 60); // 1 m
                        continue;
                    }

                    if (string.IsNullOrEmpty(envetData.actorRunId))
                    {
                        await Task.Delay(1000 * 60); // 1 m
                        continue;
                    }


                    var runRsult = await getRunResult(envetData.actorRunId);

                    if (runRsult?.run_status == "SUCCEEDED")
                    {
                        if (runRsult.data.Count > 0)
                        {
                            foreach (var _item in runRsult.data)
                            {
                                _item.Set("CrawGgVersionCode", versionCode);
                            }
                            var _placeIds = await _googleRawRepo.UpsertCrawGoogleData(runRsult.data);

                            var rawGGs = await _googleRawRepo.GetCrawlerGoogles(_placeIds.ToArray());
                            var mappingPlaces = await _googleRawRepo.GetMappingTrackings(searchTexts);
                            foreach (var mappingPlace in mappingPlaces)
                            {
                                var _rawGGs = rawGGs.Where(x => x.searchString == mappingPlace.SearchText).ToList();
                                if (_rawGGs == null)
                                {
                                    mappingPlace.IsIgnoreMapping = true;
                                }
                                else
                                {
                                    var searchPlaces = new List<AutocompletePlace>();
                                    foreach (var _rawGG in _rawGGs)
                                    {
                                        if (_rawGG.location != null)
                                        {
                                            var distance = DistanceCalculator.CalculateDistance(mappingPlace.VmLatitude, mappingPlace.VmLongitude, _rawGG.location.lat, _rawGG.location.lng) * 1000;

                                            searchPlaces.Add(new AutocompletePlace
                                            {
                                                Address = _rawGG.address,
                                                Name = _rawGG.title,
                                                Distance = distance,
                                                PlaceId = _rawGG.placeId,
                                                Coordinate = new BaseCoordinateDto
                                                {
                                                    Latitude = _rawGG.location.lat,
                                                    Longitude = _rawGG.location.lng,
                                                },
                                                GgCatName = _rawGG.categoryName,
                                            });

                                        }

                                    }

                                    var minPlace = searchPlaces.OrderBy(x => x.Distance).FirstOrDefault();
                                    if (minPlace != null)
                                    {
                                        mappingPlace.GgLat = minPlace.Coordinate.Latitude;
                                        mappingPlace.GgLng = minPlace.Coordinate.Longitude;
                                        mappingPlace.GgName = minPlace.Name;
                                        mappingPlace.GgAddrress = minPlace.Address;
                                        mappingPlace.GgPlaceId = $"googlePlaces.{minPlace.PlaceId}";
                                        mappingPlace.GgDistance = minPlace.Distance;
                                        mappingPlace.GgIsValid = minPlace.Distance < 400;
                                        mappingPlace.GgCategoryName = minPlace.GgCatName;
                                    }
                                    
                                }

                                await _googleRawRepo.UpsertMappingTracking(mappingPlace);
                            }

                            foreach (var mappingPlace in mappingPlaces)
                            {
                                await _vmPoiRawRepository.UpdatePlaceIdByVmId(mappingPlace.VmId, mappingPlace.GgPlaceId);
                            }
                        }

                        QueueRunnIds.TryRemove(envetData.actorRunId, out var removedItem);
                        _logger.LogInformation($"run id {envetData.actorRunId} updated data");
                        break;
                    }
                    else
                    {
                        _logger.LogInformation($"run id {envetData.actorRunId} not found data");
                    }

                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, ex.Message);
                }
            }
        }

        private async Task manualImportRunId(string runId, int versionCode)
        {
            var runRsult = await getRunResult(runId);

            if (runRsult?.run_status == "SUCCEEDED")
            {
                if (runRsult.data.Count > 0)
                {
                    foreach (var _item in runRsult.data)
                    {
                        _item.Set("CrawGgVersionCode", versionCode);
                    }
                    var _placeIds = await _googleRawRepo.UpsertCrawGoogleData(runRsult.data);
                }

            }

        }

        private async Task UpdateMappingManual(List<string> searchTexts)
        {
            var rawGGs = await _googleRawRepo.GetCrawlerGooglesBySearchText(searchTexts.ToArray());
            if (rawGGs?.Any() ?? false)
            {
                var mappingPlaces = await _googleRawRepo.GetMappingTrackings(searchTexts);

                foreach (var mappingPlace in mappingPlaces)
                {
                    var _rawGGs = rawGGs.Where(x => x.searchString == mappingPlace.SearchText).ToList();
                    var searchPlaces = new List<AutocompletePlace>();
                    if (_rawGGs == null || _rawGGs.Count == 0)
                    {
                        searchPlaces = new List<AutocompletePlace>();
                    }
                    else
                    {
                        foreach (var _rawGG in _rawGGs)
                        {
                            if (_rawGG.location != null)
                            {
                                var distance = DistanceCalculator.CalculateDistance(mappingPlace.VmLatitude, mappingPlace.VmLongitude, _rawGG.location.lat, _rawGG.location.lng) * 1000;

                                searchPlaces.Add(new AutocompletePlace
                                {
                                    Address = _rawGG.address,
                                    Name = _rawGG.title,
                                    Distance = distance,
                                    PlaceId = _rawGG.placeId,
                                    GgCatName = _rawGG.categoryName,
                                    Coordinate = new BaseCoordinateDto
                                    {
                                        Latitude = _rawGG.location.lat,
                                        Longitude = _rawGG.location.lng,
                                    }
                                });

                            }

                        }
                            
                    }
                    if (searchPlaces.Count > 0)
                    {

                        var minPlace = searchPlaces.OrderBy(x => x.Distance).FirstOrDefault();
                        mappingPlace.GgLat = minPlace.Coordinate.Latitude;
                        mappingPlace.GgLng = minPlace.Coordinate.Longitude;
                        mappingPlace.GgName = minPlace.Name;
                        mappingPlace.GgAddrress = minPlace.Address;
                        mappingPlace.GgPlaceId = $"googlePlaces.{minPlace.PlaceId}";
                        mappingPlace.GgDistance = minPlace.Distance;
                        mappingPlace.GgCategoryName = minPlace.GgCatName ?? string.Empty;

                        mappingPlace.GgIsValid = minPlace.Distance < 500;
                    }
                    else
                    {
                        mappingPlace.GgLat = 0.0;
                        mappingPlace.GgLng = 0.0;
                        mappingPlace.GgName = string.Empty;
                        mappingPlace.GgAddrress = string.Empty;
                        mappingPlace.GgPlaceId = string.Empty;
                        mappingPlace.GgDistance = 0.0;
                        mappingPlace.GgCategoryName = string.Empty;

                        mappingPlace.GgIsValid = false;
                    }

                    await _googleRawRepo.UpsertMappingTracking(mappingPlace);
                }

                foreach (var mappingPlace in mappingPlaces)
                {
                    await _vmPoiRawRepository.UpdatePlaceIdByVmId(mappingPlace.VmId, mappingPlace.GgPlaceId);
                }

            }

        }

        async Task<RunInfo> createTaskAndStartRun(IEnumerable<string> searchTexts)
        {
            try
            {
                var resp = await _peakClient.PostAsync("/api/crw/create_task_and_run_crawl_gg"
                    , new StringContent(JsonConvert.SerializeObject(searchTexts, Formatting.None)
                    , Encoding.UTF8, "application/json"));

                if (resp.IsSuccessStatusCode)
                {
                    var content = await resp.Content.ReadAsStringAsync();
                    var data = JsonConvert.DeserializeObject<CreateTaskAndRunResp>(content);

                    return data?.run_info;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
            }

            return null;
        }

        async Task<RunResult> getRunResult(string run_id)
        {
            try
            {
                var rsp = await _peakClient.GetAsync($"/api/crw/runs/{run_id}/data");
                if (rsp.IsSuccessStatusCode)
                {
                    var content = await rsp.Content.ReadAsStringAsync();

                    var result = JsonConvert.DeserializeObject<RunResult>(content, new JsonSerializerSettings
                    {
                        Converters = new List<JsonConverter> { new BsonDocumentConverter() }
                    });


                    return result;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
            }

            return null;
        }

        async Task<TaskingRunningResponse?> getTaskRunning()
        {
            try
            {
                var rsp = await _peakClient.GetAsync($"/api/crw/get_task_running?limit=10&offset=0&desc=true");
                if (rsp.IsSuccessStatusCode)
                {
                    var content = await rsp.Content.ReadAsStringAsync();

                    var result = JsonConvert.DeserializeObject<TaskingRunningResponse>(content);

                    return result;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
            }

            return null;
        }
    }

}
