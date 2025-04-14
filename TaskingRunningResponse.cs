using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Platform.IOTHub.ImportPOI.Service.Models.PeakApi
{
    public class TaskingRunningResponse
    {
        public List<TaskingRunning> runs { get; set; }

        public int total { get; set; }

        public int offset { get; set; }

        public int count { get; set; }
    }

    public class TaskingRunning
    {
        public string id { get; set; }

        public string task_id { get; set; }

        public string status { get; set; }
    }
}
