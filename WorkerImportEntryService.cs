using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Platform.IOTHub.ImportPOI.Service.Helper;
using Platform.IOTHub.ImportPOI.Service.Models;
using Platform.IOTHub.Repository.POI.Entities;
using Platform.IOTHub.Repository.POI.Models;
using Platform.IOTHub.Repository.POI.Repositories;
using Platform.IOTHub.Repository.VMPOIRaw;
using Platform.IOTHub.Repository.VMPOIRaw.Entities;
using Platform.IOTHub.Repository.VMPOIRaw.Models;
using Platform.IOTHub.Utility;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Drawing.Printing;
using System.Linq;
using System.Net.WebSockets;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;
using Vietmap.NetCore.Legacy.Common;
using Vietmap.NetCore.MongoDb;

namespace Platform.IOTHub.ImportPOI.Service
{
    public class WorkerImportEntryService : BackgroundService
    {
        private readonly ILogger<WorkerImportVMDsService> _logger;
        private readonly IServiceProvider _serviceProvider;


        private readonly IVMPoiRawRepository _vmPoiRawRepository;

        private readonly List<VMMappingConfig> _vmMappings;

        public WorkerImportEntryService(ILogger<WorkerImportVMDsService> logger
            , IOptions<List<VMMappingConfig>> vmMappings
            , IServiceProvider serviceProvider
            , IVMPoiRawRepository vmPoiRawRepository)
        {
            _logger = logger;

            _vmMappings = vmMappings.Value;

            _serviceProvider = serviceProvider;
            _vmPoiRawRepository = vmPoiRawRepository;

        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                //7. import entry pois
                List<Task> tasks = new List<Task>();
                int skip = 0;
                int pageSize = 100;
                while (!stoppingToken.IsCancellationRequested)
                {
                    var rawEntryPois = await _vmPoiRawRepository.GetEntryPois(skip, pageSize);
                    if (!rawEntryPois.Any())
                    {
                        break;
                    }

                    List<RawEntryPOI> upsertEntryPOIs = new List<RawEntryPOI>();
                    List<RawEntryPOI> deletedEntryPOIs = new List<RawEntryPOI>();

                    foreach (var rawEntryPoi in rawEntryPois)
                    {
                        upsertEntryPOIs.Add(rawEntryPoi);
                    }

                    if (upsertEntryPOIs.Count > 0) { 
                        await importEntryPois(rawEntryPois);
                    }

                    if (deletedEntryPOIs.Count > 0) { 
                        await removeEntryPois(rawEntryPois);
                    }


                    _logger.LogInformation($"import entry pois skip {skip} size {pageSize}");
                    skip++;
                }
                _logger.LogInformation("import entry pois");


                _logger.LogInformation("Done");
                Environment.Exit(0);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
                Environment.Exit(1);
            }

        }

        private async Task importEntryPois(IEnumerable<RawEntryPOI> rawEntryPois)
        {
            using var scope = _serviceProvider.CreateScope();
            IPoiRepository _poiRepository = scope.ServiceProvider.GetRequiredService<IPoiRepository>();

            Models.ImportSummary importSummary = new Models.ImportSummary()
            {
                Namne = "EntryPoi",
            };

            List<ImportStatus> importStatuses = new List<ImportStatus>();

            foreach (var rawEntryPoi in rawEntryPois)
            {
                try
                {
                    MappingVM? parentMappingPoi = await _poiRepository.GetMappingVM(rawEntryPoi.VMParentId);
                    if (parentMappingPoi == null)
                    {
                        importStatuses.Add(new ImportStatus()
                        {
                            Id = rawEntryPoi.Id,
                            Status = PoiImportStatus.Error.ToString(),
                            Msg = PoiImportErrorMsg.EntryPointNotFoundParentId.ToString()
                        });
                        continue;
                    }

                    //todo: add relations
                    //check imported POI 
                    string partyId = $"vml.n_{ConvertHelper.ComputeSha256Hash(rawEntryPoi.VMId.ToString())}";
                    await _poiRepository.DeleteEntryPOi(partyId);

                    Guid partyTypeId = new Guid("98040191-63bf-42eb-b613-bac03c0b8087");
                    var party = new Party()
                    {
                        Id = partyId,
                        PartyTypeId = partyTypeId,
                    };

                    party.PartyAffiliations = new List<PartyAffiliation>(){
                            new PartyAffiliation
                            {
                                Id = Guid.NewGuid(),
                                PartyId = partyId,
                                ParentPartyId = parentMappingPoi.PartyId,
                            }
                        };

                    string status = (rawEntryPoi.Status?.ToString() ?? "").ToUpper();
                    var newPoi = new EntryPOI
                    {
                        Id = partyId,
                        Name = rawEntryPoi.Name ?? "",
                        Address = rawEntryPoi.Address ?? "",
                        Label = $"{parentMappingPoi.Name} {rawEntryPoi.Name}",
                        Lat = rawEntryPoi.Lat,
                        Lng = rawEntryPoi.Lng,
                        GooglePlaceId = string.Empty,
                        IsDelete = status == ImportPOIStatus.DELETE.ToString()    
                    };

                    await _poiRepository.CreateNewEntryPOI(newPoi, party);

                    importStatuses.Add(new ImportStatus()
                    {
                        Id = rawEntryPoi.Id,
                        Status = PoiImportStatus.Success.ToString()
                    });
                }
                catch (Exception ex)
                {
                    _logger.LogError($"ImportFail::{rawEntryPoi.Name}");
                    importStatuses.Add(new ImportStatus()
                    {
                        Id = rawEntryPoi.Id,
                        Status = PoiImportStatus.Error.ToString(),
                        Msg = PoiImportErrorMsg.UnHandleError.ToString()
                    });

                    _logger.LogError(ex, ex.Message);
                }
            }

            importSummary.Total = rawEntryPois.Count();
            importSummary.TotalAddedNew = importStatuses.Where(x => x.Status == PoiImportStatus.Success.ToString()).Count();
            importSummary.TotalIgnore = importStatuses.Where(x => x.Status == PoiImportStatus.Updated.ToString()).Count();
            importSummary.TotalError = importStatuses.Where(x => x.Status == PoiImportStatus.Error.ToString()).Count();

            foreach (var rawPoi in rawEntryPois)
            {
                rawPoi.ImportStatus = importStatuses.FirstOrDefault(x => x.Id == rawPoi.Id) ?? new ImportStatus();
                await _vmPoiRawRepository.UpdateEntryPoi(rawPoi);
            }

            _logger.LogInformation($"importPois::summary::{JsonConvert.SerializeObject(importSummary)}");
            _logger.LogInformation($"importPois::errors::{JsonConvert.SerializeObject(importStatuses.Where(x => x.Status == PoiImportStatus.Error.ToString()).Select(x => new { id = x.Id, error = x.Msg }).ToList())}");
        }

        private async Task removeEntryPois(IEnumerable<RawEntryPOI> rawEntryPois)
        {
            using var scope = _serviceProvider.CreateScope();
            IPoiRepository _poiRepository = scope.ServiceProvider.GetRequiredService<IPoiRepository>();
            
            List<string> removedIds = new List<string>();
            foreach (var rawEntryPoi in rawEntryPois) {
                string partyId = $"vml.n_{ConvertHelper.ComputeSha256Hash(rawEntryPoi.VMId.ToString())}";
                removedIds.Add(partyId);
            }
            await _poiRepository.SetStatusDeleteEntryPois(removedIds);

        }
    }
}
