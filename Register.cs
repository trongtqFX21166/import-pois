using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Vietmap.NetCore.MongoDb;

namespace Platform.IOTHub.Repository.VMPOIRaw
{
    public static class Register
    {
        public static string DatabaseName { get; set; }

        public static void RegisterVmPOIRawRepo(this IServiceCollection services, HostBuilderContext context, string dbName)
        {
            services.Configure<MongoDbSettings>("VMPoiRawMongoDbSettings", context.Configuration.GetSection("VMPoiRawMongoDbSettings"));
            services.AddSingleton<IMongoDbHelper>((service) =>
            {
                var settings = service.GetRequiredService<IOptionsMonitor<MongoDbSettings>>().Get("VMPoiRawMongoDbSettings");

                DatabaseName = dbName;
                settings.DatabaseName = dbName;
                return new MongoDbHelper(settings);
            });

            services.AddSingleton<IVMPoiRawRepository, VMPoiRawRepository>();
        }
    }
}
