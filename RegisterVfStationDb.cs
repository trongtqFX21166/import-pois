using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Vietmap.NetCore.MongoDb;

namespace Platform.IOTHub.Repository.VMPOIRaw
{
    public static class RegisterVfStationDb
    {
        public static string DatabaseName { get; set; }

        public static void RegisterVinfastStationDb(this IServiceCollection services, HostBuilderContext context)
        {
            services.Configure<MongoDbSettings>("VfStationDbMongoDbSettings", context.Configuration.GetSection("VfStationDbMongoDbSettings"));
            services.AddSingleton<IMongoDbHelper>((service) =>
            {
                var settings = service.GetRequiredService<IOptionsMonitor<MongoDbSettings>>().Get("VfStationDbMongoDbSettings");

                settings.DatabaseName = "VfStationDB";
                DatabaseName = "VfStationDB";
                return new MongoDbHelper(settings);
            });

            services.AddSingleton<IVinfastStationRepo, VinfastStationRepo>();
        }
    }
}
