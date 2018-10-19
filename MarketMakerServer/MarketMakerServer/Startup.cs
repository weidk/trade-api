using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MarketMakerServer.Functions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.HttpsPolicy;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MarketMakerServer
{
    public class Startup
    {
        public static IHubContext<Hubs.MarketMakerHub> _hubContext;
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;

            QueryQuote.AsynFreshQuote();
            Orders.AsynFreshOrder();
            Orders.AsynFreshDeal();
        }
        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddSignalR();
            services.AddMvc().SetCompatibilityVersion(CompatibilityVersion.Version_2_1);
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
            app.UseSignalR(routes =>
            {
                routes.MapHub<Hubs.MarketMakerHub>("/marketmakerapi/quote");
            });
            app.UseMvc();
            _hubContext = serviceProvider.GetService<IHubContext<Hubs.MarketMakerHub>>();
        }
    }
}
