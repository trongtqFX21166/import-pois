using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Platform.IOTHub.ImportPOI.Service.Models
{
    public class ImportSummary
    {
        public string Namne { get; set; }

        public int Total { get; set; }

        public int TotalError { get; set; }

        public int TotalIgnore { get; set; }

        public int TotalUpdated { get; set; }

        public int TotalAddedNew { get; set; }
    }
}
