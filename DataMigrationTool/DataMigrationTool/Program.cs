using DataMigrationTool;
using ConsoleAppFramework;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

Console.CancelKeyPress += (sender, eventArgs) =>
{
    eventArgs.Cancel = true;
    DataMigrationToolApp.IsCancelled = true;
};

await CreateHostBuilder(args)
    .RunConsoleAppFrameworkAsync<DataMigrationToolApp>(args);

static IHostBuilder CreateHostBuilder(string[] args) =>
    Host.CreateDefaultBuilder()
    .ConfigureServices((_, services) =>
    {
    services.AddOptions();
    services.AddTransient<DataMigrationToolApp>();
    });