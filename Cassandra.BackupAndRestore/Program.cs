using Cassandra.BackupAndRestore.Base.Contracts.Helpers;
using Cassandra.BackupAndRestore.Base.Contracts.Services;
using Cassandra.BackupAndRestore.Base.Models.Configuration;
using Cassandra.BackupAndRestore.Services.Cassandra;
using Cassandra.BackupAndRestore.Utils.Helpers;
using Cassandra.BackupAndRestore.Utils.Helpers.Alerters;
using Cassandra.BackupAndRestore.Utils.Helpers.Loggers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using System;
using System.IO;
using System.Threading.Tasks;

namespace Cassandra.BackupAndRestore
{
    class Program
    {
        public static async Task Main(string[] args)
        {
            var configurationBuilder = new ConfigurationBuilder();
            configurationBuilder.AddJsonFile("appsettings.json", false);
            var configuration = configurationBuilder.Build();

            var services = new ServiceCollection();
            services.AddSingleton<ILogger, ConsoleLogger>();
            services.AddSingleton<ICounter, Counter>();
            services.AddSingleton<ICassandraService, CassandraService>();
            services.AddSingleton<IPatternMatching, PatternMatching>();
            services.AddSingleton<Runner>();
            services.AddSingleton<IConfiguration>(configuration);

            #region Configuration registering
            services.AddSingleton<CassandraConfiguration>();
            services.Configure<CassandraConfiguration>(configuration.GetSection("Cassandra"));
            services.AddSingleton(resolver => resolver.GetRequiredService<IOptions<CassandraConfiguration>>().Value);

            services.AddSingleton<AlertingConfiguration>();
            services.Configure<AlertingConfiguration>(configuration.GetSection("Alerting"));
            services.AddSingleton(resolver => resolver.GetRequiredService<IOptions<AlertingConfiguration>>().Value);

            services.AddSingleton<BackupConfiguration>();
            services.Configure<BackupConfiguration>(configuration.GetSection("Backup"));
            services.AddSingleton(resolver => resolver.GetRequiredService<IOptions<BackupConfiguration>>().Value);

            services.AddSingleton<BackupArchivingConfiguration>();
            services.Configure<BackupArchivingConfiguration>(configuration.GetSection("BackupArchiving"));
            services.AddSingleton(resolver => resolver.GetRequiredService<IOptions<BackupArchivingConfiguration>>().Value);
            #endregion Configuration registering

            var serviceProvider = services.BuildServiceProvider();

            var alertingConfiguration = serviceProvider.GetService<AlertingConfiguration>();
            switch(alertingConfiguration.Type)
            {
                default:
                case "OpsGenie":
                    services.AddSingleton<IAlerter, OpsGenieAlerter>();
                    break;
            }

            serviceProvider = services.BuildServiceProvider();
            var runner = serviceProvider.GetService<Runner>();
            await runner.RunAsync();
        }
    }
}
