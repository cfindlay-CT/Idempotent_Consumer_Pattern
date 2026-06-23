using Microsoft.Extensions.Caching.Distributed;
using StackExchange.Redis;

public interface IRedisCacheService
{
    Task<bool> AcquireLockAsync(string messageId, TimeSpan? expiration = null);
    Task MarkAsCompletedAsync(string messageId, TimeSpan retentionPeriod);
    Task ReleaseLockAsync(string messageId);}

public class RedisCacheService : IRedisCacheService
{
    private readonly IConnectionMultiplexer _connectionMultiplexer;
    private readonly IDatabase _cache;

    public RedisCacheService(IConnectionMultiplexer connectionMultiplexer)
    {
        _connectionMultiplexer = connectionMultiplexer;
        _cache = _connectionMultiplexer.GetDatabase();
    }

    public async Task<bool> AcquireLockAsync(string messageId, TimeSpan? expiration = null)
    {
        // Try to acquire the lock
        var lockKey = $"idempotency:lock:{messageId}";
        var lockValue = Guid.NewGuid().ToString();

        var lockAcquired = await _cache.StringSetAsync(lockKey, "processing", expiration ?? TimeSpan.FromMinutes(5), When.NotExists);
        return lockAcquired;
    }

    public async Task MarkAsCompletedAsync(string messageId, TimeSpan retentionPeriod)
    {
        var lockKey = $"idempotency:lock:{messageId}";
         await _cache.StringSetAsync(lockKey, "completed", retentionPeriod);

    }

    public async Task ReleaseLockAsync(string messageId)
    {
        var lockKey = $"idempotency:lock:{messageId}";
        await _cache.KeyDeleteAsync(lockKey);
    }
}