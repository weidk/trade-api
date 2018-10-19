using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Cors.Infrastructure;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.HttpsPolicy;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using RealtimeApi.Funcs;

namespace RealtimeApi
{
    public class Startup
    {
        private IHubContext<Hubs.BpchangeHub> _hubContext;
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
            DateTime endTime = new DateTime(DateTime.Now.Year, DateTime.Now.Month, DateTime.Now.Day, 23, 30, 00);
            Task.Run(() =>
            {
                while (true)
                {
                    Thread.Sleep(1000);
                    try
                    {
                        if (MonitorBp.CalChangeBp(Config.Bonds))
                        {
                            var ToSendJson = MonitorBp.GetJsonString(Config.BpChangeDict);
                            _hubContext.Clients.All.SendAsync("ReceiveBp", ToSendJson);
                        }
                        if (DateTime.Now > endTime)
                        {
                            System.Environment.Exit(0);
                        }
                    }
                    catch (Exception ex) {
                        Console.WriteLine(ex.ToString());
                    }
                }
            });
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddSignalR();
            services.AddMvc().SetCompatibilityVersion(CompatibilityVersion.Version_2_1);
            //services.AddCors(options => options.AddPolicy("CorsPolicy",
            //builder =>
            //{
            //    builder.AllowAnyMethod().AllowAnyHeader().AllowAnyOrigin()
            //           //.WithOrigins(Config.FrontUrl)
            //           .AllowCredentials();
            //}));
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env, IServiceProvider serviceProvider)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }
            else
            {
                app.UseHsts();
            }

            //app.UseHttpsRedirection();

            //app.UseCors("CorsPolicy");
            app.UseSignalR(routes =>
            {
                routes.MapHub<Hubs.BpchangeHub>("/signalrapi/bpchange");
            });
            app.UseMvc();
            _hubContext = serviceProvider.GetService<IHubContext<Hubs.BpchangeHub>>();
        }
    }
}
