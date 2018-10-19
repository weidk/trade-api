using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using RealtimeApi.Funcs;
using RealtimeApi.Hubs;

namespace RealtimeApi
{
    public class Program
    {
        public static void Main(string[] args)
        {
            #region 读取配置文件
            
            var builder = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory()).AddJsonFile("appsettings.json");
            var config = builder.Build();

            Config.FrontUrl = config["FrontUrl"];
            Config.Conn43 = config["Conn43"];
            Config.ConnMSN = config["ConnMSN"];
            Config.Bonds = config["CBonds"];
            Config.CBonds = MonitorBp.CBondValues(Config.ConnMSN, config["CBonds"]);
            Config.db = new SqlConnection(Config.Conn43);
            Config.db.Open();
            Config.BpChangeDict = MonitorBp.TransCodeString(config["CBonds"]);
            Config.DealYieldDict = MonitorBp.TransCodeString(config["CBonds"]);
            MonitorBp.GetTermDict(config["Terms:10y"],"10y");
            MonitorBp.GetTermDict(config["Terms:5-7y"], "5-7y");
            MonitorBp.GetTermDict(config["Terms:1-3y"], "1-3y");
            Config.BondsTypeDict = MonitorBp.GetTypeDict(config["CBonds"]);
            #endregion
            CreateWebHostBuilder(args).Build().Run();
            
        }

        public static IWebHostBuilder CreateWebHostBuilder(string[] args) =>
            WebHost.CreateDefaultBuilder(args)
                .UseStartup<Startup>();
    }
}
