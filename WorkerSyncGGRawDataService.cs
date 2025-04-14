using Amazon.Runtime;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Platform.IOTHub.Common.Models.JimiHub;
using Platform.IOTHub.ImportPOI.Service.Models.PeakApi;
using Platform.IOTHub.Repository.POI.Repositories;
using Platform.IOTHub.Repository.VMPOIRaw;
using Platform.KafkaClient;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Platform.IOTHub.ImportPOI.Service
{
    public class WorkerSyncGGRawDataService : BackgroundService
    {
        private readonly IGoogleRawRepo _googleRawRepo;
        private readonly IServiceProvider _serviceProvider;

        private readonly ILogger<WorkerSyncGGRawDataService> _logger;
        private readonly HttpClient _peakClient;
        private readonly IConsumer _consumer;

        public WorkerSyncGGRawDataService(
            IGoogleRawRepo googleRawRepo
            , IServiceProvider serviceProvider
            , ILogger<WorkerSyncGGRawDataService> logger
            , IHttpClientFactory httpClientFactory
            , IConsumer consumer)
        {
            _googleRawRepo = googleRawRepo;
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
                && msg.eventData != null)
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
            var _taskRunnings = await getTaskRunning();
            if(_taskRunnings?.runs?.Any(x => x.status == "RUNNING") ?? false)
            {
                foreach(var  run in _taskRunnings.runs)
                {
                    QueueRunnIds.TryAdd(run.task_id, new PeakApiEventData { });
                    await WaitAndProccessImport(_poiRepository, new RunInfo
                    {
                        task_id = run.task_id,
                    }, versionCode);
                }
            }


            //3. get total placeIds with new versionCode
            int pageSize = 500;
            //string run_id = "yEMl8of12fCQoeWSn";
            var total = await _poiRepository.GetTotalCrawGGPois(versionCode);
            var pageCount = total / pageSize;

            //4. get all placeIds with new version Code
            List<string> _allRawPlaceIds = new List<string>();
            for (int skip = 0; skip < pageCount; skip++)
            {
                IEnumerable<string> _data = await _poiRepository.GetCrawGGPois(versionCode, skip, pageSize);
                if (_data.Any())
                {
                    _allRawPlaceIds.AddRange(_data);
                }
            }

           
            _allRawPlaceIds = _allRawPlaceIds.Select(x => x.Replace("googlePlaces.", "")).Distinct().ToList();
            for (int skip = 0; skip < pageCount; skip++)
            {
                //5. send craw placids
                var placeIds = _allRawPlaceIds.Skip(skip * pageSize).Take(pageSize).ToList();

                RunInfo runInfo = await createTaskAndStartRun(placeIds.Select(x => $"placeid:{x}"));
                if (runInfo == null
                    || string.IsNullOrEmpty(runInfo.run_id))
                {
                    await Task.Delay(1000);
                    _logger.LogInformation($"Cannot start start run skip {skip} pagesize {pageSize}");
                    continue;
                }

                _logger.LogInformation($"Started start run skip {skip} pagesize {pageSize}");
                QueueRunnIds.TryAdd(runInfo.task_id, new PeakApiEventData());

                //6. wait and update version code
                await WaitAndProccessImport(_poiRepository, runInfo, versionCode);
            }


            _logger.LogInformation("done");
        }

        private async Task WaitAndProccessImport(IPoiRepository poiRepository, RunInfo _runInfo, int versionCode)
        {
            int count = 0;
            while (true)
            {
                count++;
                if (count > 60)
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

                            await poiRepository.UpdateCrawGGPoiVersion(versionCode, _placeIds.Select(x => $"googlePlaces.{x}").ToArray());
                        }

                        QueueRunnIds.TryRemove(envetData.actorRunId, out var removedItem);
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

        private async Task manualImportRunId(IPoiRepository poiRepository, string runId)
        {
            var runRsult = await getRunResult(runId);

            if (runRsult?.run_status == "SUCCEEDED")
            {
                if (runRsult.data.Count > 0)
                {
                    //foreach (var _item in runRsult.data)
                    //{
                    //    _item.CrawGgVersionCode = "20241108";
                    //}
                    var _placeIds = await _googleRawRepo.UpsertCrawGoogleData(runRsult.data);

                    await poiRepository.UpdateCrawGGPoiVersion(20241108, _placeIds.Select(x => $"googlePlaces.{x}").ToArray());
                }
            }
        }


        async Task<RunInfo> createTaskAndStartRun(IEnumerable<string> placeids)
        {
            try
            {
                var resp = await _peakClient.PostAsync("/api/crw/create_task_and_run_crawl_gg"
                    , new StringContent(JsonConvert.SerializeObject(placeids, Formatting.None)
                    , Encoding.UTF8, "application/json"));

                if (resp.IsSuccessStatusCode)
                {
                    var content = await resp.Content.ReadAsStringAsync();
                    var data = JsonConvert.DeserializeObject<CreateTaskAndRunResp>(content);

                    return data?.run_info;
                }
            }catch(Exception ex)
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
            }catch(Exception ex)
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
            }catch(Exception ex)
            {
                _logger.LogError(ex, ex.Message);
            }

            return null;
        }
    }

    public class BsonDocumentConverter : JsonConverter
    {
        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(BsonDocument);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            var jObject = JObject.Load(reader);
            return BsonDocument.Parse(jObject.ToString());
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            var bsonDocument = value as BsonDocument;
            writer.WriteRawValue(bsonDocument.ToJson());
        }
    }
}
