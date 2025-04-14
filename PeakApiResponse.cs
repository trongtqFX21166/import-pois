using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Platform.IOTHub.ImportPOI.Service.Models.PeakApi
{
    public class PeakApiResponse<T> where T : class
    {
        public int code { get; set; }
        public string message { get; set; }

        public T data { get; set; }
    }
}
