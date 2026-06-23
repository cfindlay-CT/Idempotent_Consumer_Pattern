using System;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace demo.functions;

/// <summary>
/// Service Bus-triggered idempotent consumer. Guards against duplicate processing that arises
/// from at-least-once delivery semantics: broker redelivery after a lock-renewal timeout,
/// client retries on ambiguous (timeout) acks, and competing concurrent dispatch when the
/// function app scales out to multiple instances reading the same subscription.
///
/// The dedup decision is delegated entirely to <see cref="IRedisCacheService"/> so this trigger
/// never has to reason about distributed locking itself — it only reacts to whether the claim
/// succeeded.
/// </summary>
public class IdempotentConsumerTrigger
{
    private readonly ILogger<IdempotentConsumerTrigger> _logger;
    private readonly IRedisCacheService _redisCacheService;

    public IdempotentConsumerTrigger(IRedisCacheService redisCacheService, ILogger<IdempotentConsumerTrigger> logger)
    {
        _redisCacheService = redisCacheService;
        _logger = logger;
    }

//serviceBusConnection
    [Function(nameof(IdempotentConsumerTrigger))]
    public async Task Run(
        [ServiceBusTrigger("demo", Connection = "serviceBusConnection")]
        ServiceBusReceivedMessage message,
        ServiceBusMessageActions messageActions)
    {

        if(string.IsNullOrEmpty(message.MessageId))
        {
            // Without a MessageId there is no dedup key to lock on, so this message can never be
            // safely identified as a duplicate or an original. Dead-lettering immediately keeps it
            // from being retried indefinitely against a pattern that has no way to evaluate it.
            _logger.LogWarning("Received message with null or empty MessageId. Skipping processing.");
            await messageActions.DeadLetterMessageAsync(message, deadLetterReason: "Invalid MessageId", deadLetterErrorDescription: "MessageId is null or empty.");
            return;
        }

        _logger.LogInformation("Processing transaction for Message {MessageId} received from queue.", message.MessageId);
        _logger.LogInformation("Message Body: {body}", message.Body);
        _logger.LogInformation("Message Content-Type: {contentType}", message.ContentType);

        // Try to acquire the lock
        bool lockAcquired = await _redisCacheService.AcquireLockAsync(message.MessageId);
        if (!lockAcquired)
        {
            // Another instance already holds (or has completed) this MessageId's claim. Completing
            // rather than dead-lettering or abandoning is intentional: this is the expected, healthy
            // outcome of at-least-once delivery, not an error condition, so it should not count
            // against the message's max-delivery-count or trigger alerting.
            _logger.LogInformation("Message {id} is a duplicate.", message.MessageId);
            await messageActions.CompleteMessageAsync(message);
            return;
        }

        try
        {
            // Process the message
            _logger.LogInformation("Processing message: {id}", message.MessageId);

            // Mark the message as completed and release the lock
            await _redisCacheService.MarkAsCompletedAsync(message.MessageId, TimeSpan.FromMinutes(10));

        }
        catch (Exception ex)
        {
            // The lock is released explicitly on failure rather than left to expire on its TTL.
            // This is what distinguishes a transient processing error from a poison message:
            // releasing immediately lets Service Bus redeliver and retry right away, while the
            // re-throw still lets the broker's max-delivery-count eventually dead-letter a message
            // that keeps failing, instead of looping forever.
            await _redisCacheService.ReleaseLockAsync(message.MessageId);
            _logger.LogError(ex, "Error processing message {id}.", message.MessageId);
            throw; // Re-throw the exception to ensure the message is not marked as completed
        }

        // Complete the message
        await messageActions.CompleteMessageAsync(message);
    }
}