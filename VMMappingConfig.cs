using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Platform.IOTHub.ImportPOI.Service.Models
{
    public class VMMappingConfig
    {
        public Guid VMLCatId { get; set; }

        public string VMLCatCode { get; set; }

        public IEnumerable<string> VMCatCodes { get; set; }

        public IEnumerable<VMMappingConfigItem> BrandMappings { get; set; }
    }

    public class VMMappingConfigItem
    {
        public Guid VMLId { get; set; }

        public string VMLCode { get; set; }

        public IEnumerable<string> VMCodes { get; set; }
    }
}
