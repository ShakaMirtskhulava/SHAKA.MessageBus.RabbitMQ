using MessageBus.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace MessageBus.RabbitMQ;

public class EventBusBuilder(IServiceCollection services) : IEventBusBuilder
{
    public IServiceCollection Services => services;
}
