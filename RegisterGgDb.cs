using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using System.Runtime.CompilerServices;
using Vietmap.NetCore.MongoDb;

namespace Platform.IOTHub.Repository.VMPOIRaw
{
    public static class RegisterGgDb
    {
        public static string DatabaseName { get; set; }

        public static void RegisterGoogleDb(this IServiceCollection services, HostBuilderContext context)
        {
            services.Configure<MongoDbSettings>("GoogleDbMongoDbSettings", context.Configuration.GetSection("GoogleDbMongoDbSettings"));
            services.AddSingleton<IMongoDbHelper>((service) =>
            {
                var settings = service.GetRequiredService<IOptionsMonitor<MongoDbSettings>>().Get("GoogleDbMongoDbSettings");

                settings.DatabaseName = "GoogleDB";
                DatabaseName = "GoogleDB";
                return new MongoDbHelper(settings);
            });

            services.AddSingleton<IGoogleRawRepo, GoogleRawRepo>();
        }
    }
}
