using System;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace demo.functions;

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
            await _redisCacheService.ReleaseLockAsync(message.MessageId);
            _logger.LogError(ex, "Error processing message {id}.", message.MessageId);
            throw; // Re-throw the exception to ensure the message is not marked as completed
        }

        // Complete the message
        await messageActions.CompleteMessageAsync(message);
    }
}