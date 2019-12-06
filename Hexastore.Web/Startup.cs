using System.IO;
using dotenv.net;
using Hexastore.Errors;
using Hexastore.Processor;
using Hexastore.Resoner;
using Hexastore.Rocks;
using Hexastore.Store;
using Hexastore.Web.EventHubs;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Hexastore.Web
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            // services.AddMvc().SetCompatibilityVersion(CompatibilityVersion.Version_2_2);
            services.AddCors(options =>
            {
                options.AddPolicy("AllowAnyOrigin",
                    builder => builder.AllowAnyOrigin());
            });

            services.AddMvc();
            services.AddSingleton<IGraphProvider, RocksGraphProvider>();
            services.AddSingleton<IReasoner, Reasoner>();
            services.AddSingleton<IStoreProcesor, StoreProcessor>();
            services.AddSingleton<IStoreProvider, SetProvider>();
            services.AddSingleton<IStoreOperationFactory, StoreOperationFactory>();

            services.AddSingleton<StoreError>();
            services.AddSingleton<EventReceiver>();
            services.AddSingleton<EventSender>();
            services.AddSingleton<Checkpoint>();
            services.AddSingleton<StoreConfig>();
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env)
        {
            if (File.Exists("/var/data/Hexastore.env")) {
                DotEnv.Config(false, "/var/data/Hexastore.env");
            } else if (File.Exists("Hexastore.env")) {
                DotEnv.Config(false, "Hexastore.env");
            }

            if (env.IsDevelopment()) {
                app.UseDeveloperExceptionPage();
            }
            app.UseCors("AllowAnyOrigin");

            app.UseMiddleware<PerfHeaderMiddleware>();
            app.UseMvc();
        }
    }
}
