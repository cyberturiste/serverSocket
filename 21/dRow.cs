using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MessageServer
{
    [Serializable]
    class dRow
    {

        public int district_id { get; set; }
        public string district_name { get; set; }
        public int district_area { get; set; }
        public int town_id { get; set; }
        public string town_name { get; set; }
        public int town_population { get; set; }
        public int street_id { get; set; }
        public string street_name { get; set; }
        public int house_id { get; set; }
        public int house_countfloors { get; set; }
        public int apart_id { get; set; }
        public int apart_countrooms { get; set; }
        public int apart_area { get; set; }
    }
}
