## **MessageBus.RabbitMQ Project**

The second project is **MessageBus.RabbitMQ**, which is a specific implementation of the EventBus using **RabbitMQ** as the message broker.

The main class here is **RabbitMQEventBus**, which implements both **IEventBus** and **IHostedService**. This means it serves as both an event bus and a background job. Its primary responsibility is to **establish a connection** to RabbitMQ as soon as it starts. It then declares exchanges, queues, and binds them for each **SubscriptionInfo**. Some projects may not require any subscriptions. 

The **OnMessageReceived** method is responsible for registering event handlers for specific events retrieved from **SubscriptionInfo**. This method then calls **ProcessEvent**, which resolves the event handler from the DI container using the event’s name and registers it with the broker. Withing it it calls the ProcessEvent() method which is responsible for resolving all the eventHandlers for the specific events and invoking them, right before the invokation it checks for an existance of the failedMessageChain for the given entities
EntitiyId, if there is a chain, handler won't be invoked and the event will be added in the failed message chain. If the invoked handler throws an exception, we use AddInFailedMessageChain() method to create a failed message chain and add this event as a first failed message.

Another critical method is `PublishAsync`, which takes an `IntegrationEvent`. This method retrieves the **name** and **full name** of the event and looks for an exchange with the same name. It then publishes the event there using the event name as the **routing key**.

To ensure resiliency, events are published via **ResiliencePipeline**, which allows us to configure different **resiliency strategies**. Currently, we use a **Retry Strategy** that handles `BrokerUnreachableException` and `SocketException`, with the retry count configurable via **EventBusOptions** in `appsettings.json`.

To register **RabbitMQEventBus** in the DI container as both `IEventBus` and `IHostedService`, we use the **RabbitMqDependencyInjectionExtensions** class. The extension methods also require the consumer to configure the **ConnectionFactory** for the broker.
