using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace MarketMakerServer
{
    public class Config
    {
        public static string ConnectionString87 { get; set; }
        public static string BondcodeList { get; set; }
        public static SqlConnection db;
        public static SqlConnection OrderDb;
        public static SqlConnection DealDb;
        public static SqlConnection WriteNewOrderDb;
        public static int MAXQUOTEID = 0;
        public static int MAXORDERID = 0;
        public static int MAXDEALID = 0;

        public static void Initial()
        {
            #region 读取配置文件

            var builder = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory()).AddJsonFile("appsettings.json");
            var config = builder.Build();
            Config.ConnectionString87 = config["ConnectionString87"];
            Config.BondcodeList = config["BondcodeList"];

            #endregion

            #region 连接数据库
            Config.db = new SqlConnection(Config.ConnectionString87);
            Config.db.Open();

            Config.OrderDb = new SqlConnection(Config.ConnectionString87);
            Config.OrderDb.Open();

            Config.WriteNewOrderDb = new SqlConnection(Config.ConnectionString87);
            Config.WriteNewOrderDb.Open();

            Config.DealDb = new SqlConnection(Config.ConnectionString87);
            Config.DealDb.Open();
            #endregion

        }
    }


}
