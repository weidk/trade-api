using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace RealtimeApi
{
    public class Config
    {
        public static string FrontUrl { get; set; }
        public static string ConnMSN { get; set; }
        public static string Conn43 { get; set; }
        public static string Bonds { get; set; }
        public static Dictionary<string, double> CBonds;
        public static Dictionary<string, string> BondsTermDict = new Dictionary<string, string>();
        public static Dictionary<string, string> BondsTypeDict;
        public static Dictionary<string, double> BpChangeDict;
        public static Dictionary<string, double> DealYieldDict;
        public static SqlConnection db;
        public static string Dtime = DateTime.Today.ToString("yyyy-MM-dd");
        public static int id = 0;
    }

    public class BpInfoClass
    {
        public string code { get; set; }
        public double bp { get; set; }
        public string type { get; set; }
        public string term { get; set; }
        public double yield { get; set; }
    }
}
