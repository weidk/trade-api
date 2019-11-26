using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace XBondServer
{
    public class Config
    {
        public static string ConnectionString { get; set; }
        public static SqlConnection db;
     

        public static void Initial()
        {
            #region 读取配置文件

            var builder = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory()).AddJsonFile("appsettings.json");
            var config = builder.Build();
            Config.ConnectionString = config["ConnectionString"];

            #endregion

            #region 连接数据库
            Config.db = new SqlConnection(Config.ConnectionString);
            Config.db.Open();

            #endregion

        }
    }
}
