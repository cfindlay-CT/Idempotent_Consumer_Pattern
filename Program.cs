using Azure.Monitor.OpenTelemetry.Exporter;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Builder;
using Microsoft.Azure.Functions.Worker.OpenTelemetry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using OpenTelemetry;
using StackExchange.Redis;

var builder = FunctionsApplication.CreateBuilder(args);

builder.ConfigureFunctionsWebApplication();

string redisConnection = builder.Configuration["RedisConnection"] ?? throw new InvalidOperationException("Redis connection string is not configured.");
var multiplexer = ConnectionMultiplexer.Connect(redisConnection);

builder.Services.AddSingleton<IConnectionMultiplexer>(multiplexer);
builder.Services.AddTransient<IRedisCacheService, RedisCacheService>();

if (!string.IsNullOrEmpty(builder.Configuration["APPLICATIONINSIGHTS_CONNECTION_STRING"]))
{
    builder.Services.AddOpenTelemetry()
        .UseFunctionsWorkerDefaults()
        .UseAzureMonitorExporter();
}

builder.Build().Run();
