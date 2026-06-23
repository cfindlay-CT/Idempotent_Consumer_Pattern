using Microsoft.Extensions.Caching.Distributed;
using StackExchange.Redis;

/// <summary>
/// Distributed deduplication store backed by Redis. Acts as the single source of truth
/// for "has this message already been claimed/processed" across all concurrently running
/// function instances, since each instance has no in-memory knowledge of the others.
/// </summary>
public interface IRedisCacheService
{
    Task<bool> AcquireLockAsync(string messageId, TimeSpan? expiration = null);
    Task MarkAsCompletedAsync(string messageId, TimeSpan retentionPeriod);
    Task ReleaseLockAsync(string messageId);}

/// <summary>
/// Redis-backed implementation of the idempotency lock/tracking store.
///
/// All three operations key off a single string record (<c>idempotency:lock:{messageId}</c>)
/// rather than a separate "lock" and "processed" table. This is deliberate: a single key with a
/// state value ("processing" / "completed") avoids the multi-statement read-then-write race that a
/// "SELECT exists, then INSERT" pattern has under concurrent consumers — see <c>AcquireLockAsync</c>.
/// </summary>
public class RedisCacheService : IRedisCacheService
{
    private readonly IConnectionMultiplexer _connectionMultiplexer;
    private readonly IDatabase _cache;

    public RedisCacheService(IConnectionMultiplexer connectionMultiplexer)
    {
        _connectionMultiplexer = connectionMultiplexer;
        _cache = _connectionMultiplexer.GetDatabase();
    }

    /// <summary>
    /// Atomically claims the right to process <paramref name="messageId"/>.
    ///
    /// Uses Redis <c>SET key value NX EX</c> (here via <see cref="When.NotExists"/>) instead of an
    /// "exists check" followed by a separate "set" call. A check-then-set pair is not atomic — two
    /// competing consumers (e.g. two function instances racing on the same Service Bus message during
    /// a redelivery) could both pass the existence check before either writes, and both would proceed
    /// to process the message. Folding the check and the write into one round-trip closes that window.
    ///
    /// The lock carries its own TTL (default 5 minutes) independent of the completion record's TTL.
    /// This bounds how long a message stays "claimed" if the owning instance crashes mid-processing
    /// without ever calling <see cref="ReleaseLockAsync"/> or <see cref="MarkAsCompletedAsync"/> —
    /// the lock self-expires and the message becomes eligible for reprocessing instead of being
    /// stuck "in flight" forever.
    /// </summary>
    public async Task<bool> AcquireLockAsync(string messageId, TimeSpan? expiration = null)
    {
        // Try to acquire the lock
        var lockKey = $"idempotency:lock:{messageId}";
        var lockValue = Guid.NewGuid().ToString();

        var lockAcquired = await _cache.StringSetAsync(lockKey, "processing", expiration ?? TimeSpan.FromMinutes(5), When.NotExists);
        return lockAcquired;
    }

    /// <summary>
    /// Overwrites the lock record with a "completed" marker and a longer retention TTL.
    ///
    /// Reusing the same key (rather than deleting the lock and writing a new "processed" key)
    /// guarantees there is never a gap where the key is absent between "done processing" and
    /// "marked done" — a gap during which a redelivered duplicate could slip through
    /// <see cref="AcquireLockAsync"/> and reprocess the message. The retention TTL is intentionally
    /// longer than the lock TTL: it needs to outlive the broker's message-redelivery window
    /// (e.g. Service Bus lock duration / retry backoff) so late duplicates are still recognized.
    /// </summary>
    public async Task MarkAsCompletedAsync(string messageId, TimeSpan retentionPeriod)
    {
        var lockKey = $"idempotency:lock:{messageId}";
         await _cache.StringSetAsync(lockKey, "completed", retentionPeriod);

    }

    /// <summary>
    /// Explicitly frees the claim on failure so the message is immediately eligible for retry
    /// rather than waiting out the full lock TTL. Without this, a transient processing error
    /// (e.g. a downstream dependency timeout) would block redelivery of a legitimately-failed
    /// message for the entire lock expiration window.
    /// </summary>
    public async Task ReleaseLockAsync(string messageId)
    {
        var lockKey = $"idempotency:lock:{messageId}";
        await _cache.KeyDeleteAsync(lockKey);
    }
}