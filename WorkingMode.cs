using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Platform.IOTHub.ImportPOI.Service.Models
{
    public enum WorkingMode
    {
        ImportVm,
        ImportWaze,
        ImportMaster,
        ImportAdmin,
        ImportEntry,
        ImportClicksData,
        LoadRedis,
        LoadVfPowerRedis,
        LoadEntryPoiRedis,
        AddVfPower,
        ClearCached,
        AddPlace,
        CrawPlaceByRawGG,
        ReMappingPlaceByRawGG,
        CrawVfPowers,
        AddGgData,
        CrawGGData,
        CrawWazeData,
        LoadMapCat,
        LoadPartyMapping

    }
}
