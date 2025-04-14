using System.ComponentModel.DataAnnotations.Schema;

namespace Platform.IOTHub.ImportPOI.Service.Cache
{
    public class EvsePowerCache
    {
        public int Type { get; set; }

        public int TotalEvse { get; set; }
    }
}
