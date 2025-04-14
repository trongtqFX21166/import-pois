using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Platform.IOTHub.ImportPOI.Service.Models
{
    public class AutocompletePlaceDto
    {
        public string PlaceId { get; set; }

        public string Name { get; set; }

        public string Address { get; set; }

        public string Description { get; set; }

        public BaseCoordinateDto Coordinate { get; set; }

        public double Distance { get; set; }

        public double RateSimmilar { get; set; }
    }

    public class BaseCoordinateDto
    {
        /// <summary>
        /// Gets or sets the latitude.
        /// </summary>
        /// <value>
        /// The latitude.
        /// </value>
        public double Latitude { get; set; }

        /// <summary>
        /// Gets or sets the longitude.
        /// </summary>
        /// <value>
        /// The longitude.
        /// </value>
        public double Longitude { get; set; }
    }
}
