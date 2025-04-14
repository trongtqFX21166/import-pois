using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Platform.IOTHub.ImportPOI.Service.Models
{
    public class AutocompleteWazeResponse
    {
        public double x { get; set; }

        public double y { get; set; }

        public string v { get; set; }

        public AutocompleteWazeDto o { get; set; }
    }

    public class AutocompleteWazeDto
    {
        public string d { get; set; }
    }
}
