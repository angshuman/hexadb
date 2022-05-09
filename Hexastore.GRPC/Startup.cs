namespace Hexastore.GRPC
{
    using Hexastore.Errors;
    using Hexastore.GRPC.Services;
    using Hexastore.Processor;
    using Hexastore.Resoner;
    using Hexastore.Rocks;
    using Hexastore.Store;
    using Microsoft.AspNetCore.Builder;
    using Microsoft.AspNetCore.Hosting;
    using Microsoft.AspNetCore.Http;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Hosting;

    public class Startup
    {
        // This method gets called by the runtime. Use this method to add services to the container.
        // For more information on how to configure your application, visit https://go.microsoft.com/fwlink/?LinkID=398940
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddGrpc();

            services.AddSingleton<IGraphProvider, RocksGraphProvider>();
            services.AddSingleton<IReasoner, Reasoner>();
            services.AddSingleton<IStoreProcessor, StoreProcessor>();
            services.AddSingleton<IStoreProvider, SetProvider>();
            services.AddSingleton<StoreError>();
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment()) {
                app.UseDeveloperExceptionPage();
            }

            app.UseRouting();

            app.UseEndpoints(endpoints => {

                endpoints.MapGrpcService<IngestService>();
                endpoints.MapGrpcService<QueryService>();
                endpoints.MapGrpcService<GreeterService>();

                endpoints.MapGet("/", async context => {
                    await context.Response.WriteAsync("Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
                });
            });
        }
    }
}
