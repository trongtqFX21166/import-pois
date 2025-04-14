using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Platform.IOTHub.ImportPOI.Service.Models.PeakApi
{
    public class CreateTaskAndRunResp
    {
        public TaskInfo task_info { get; set; }
        public RunInfo run_info { get; set; }
    }

    public class TaskInfo
    {
        public string task_id { get; set; }
        public string task_name { get; set; }
        public string task_status { get; set; }
    }

    public class RunInfo
    {
        public string task_id { get; set; }
        public string run_id { get; set; }
        public string run_name { get; set; }
        public string run_status { get; set; }
        public string started_at { get; set; }
    }
}
