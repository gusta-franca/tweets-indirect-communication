/*
   Data de criação: 16/11/2026
   Estudante: Caroline Lau
   Assinante de tweets do Twitter
*/

using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Serilog;

Log.Logger = new LoggerConfiguration()
    .WriteTo.Console()
    .CreateLogger();

try
{
    // Tempo máximo de espera pela resposta do serviço de mensagens
    var replyTimeout = TimeSpan.FromSeconds(5);

    string? subscriber = args.Length > 0 ? args[0] : AskQuestion("Digite o nome do assinante: ");
    if (subscriber is null)
    {
        Log.Warning("Nenhum assinante fornecido. Encerrando o programa.");
        return 1;
    }

    Log.Information("Assinante: {Subscriber}", subscriber);

    string[] topics = args.Length > 1 ? ParseTopics(string.Join(' ', args[1..])) : ParseTopics(AskQuestion("Digite os tópicos de interesse (separados por espaço ou vírgula): ") ?? string.Empty);
    if (topics.Length == 0)
    {
        Log.Warning("Nenhum tópico fornecido. Encerrando o programa.");
        return 1;
    }
    Log.Information("Tópicos de interesse: {Topics}", topics);

    string url = Environment.GetEnvironmentVariable("AMQP_URL") ?? "amqp://tweets:tweets@localhost:5672/";

    var factory = new ConnectionFactory
    {
        Uri = new Uri(url)
    };

    await using var connection = await factory.CreateConnectionAsync();
    Log.Information("Conectado ao broker!");

    await using var channel = await connection.CreateChannelAsync();
    Log.Information("Canal aberto!");

    var replyQueue = await channel.QueueDeclareAsync(queue: "", durable: false, exclusive: true, autoDelete: true);

    Log.Information("Fila de resposta criada: {QueueName}", replyQueue.QueueName);

    var request = new SubscriptionRequest("subscribe", subscriber, topics);

    byte[] body = JsonSerializer.SerializeToUtf8Bytes(request);

    string correlationId = Guid.NewGuid().ToString(); // identificador de 128 bits gerado aleatoriamente para correlacionar a resposta com a solicitação

    var properties = new BasicProperties
    {
        ReplyTo = replyQueue.QueueName,
        CorrelationId = correlationId,
        ContentType = "application/json"
    };

    await channel.BasicPublishAsync(
        exchange: "",
        routingKey: "tweets.control",
        mandatory: true, //devolve a mensagem para o remetente se não houver fila vinculada à chave de roteamento
        basicProperties: properties,
        body: body
    );

    Log.Information("Pedido enviado: {Subscriber} -> {Topics} (correlation_id={CorrelationId})", subscriber, topics, correlationId);

    var replyReceived = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

    var consumer = new AsyncEventingBasicConsumer(channel);
    consumer.ReceivedAsync += (sender, ea) =>
    {
        if (ea.BasicProperties.CorrelationId == correlationId)
        {
            string response = Encoding.UTF8.GetString(ea.Body.Span);
            replyReceived.TrySetResult(response);
        }
        else
        {
            Log.Warning("Mensagem recebida com correlation_id diferente: {CorrelationId}", ea.BasicProperties.CorrelationId);
        }
        return Task.CompletedTask;
    };

    await channel.BasicConsumeAsync(
        queue: replyQueue.QueueName,
        autoAck: true,
        consumer: consumer
    );

    string replyJson;

    try
    {
        replyJson = await replyReceived.Task.WaitAsync(replyTimeout);
    }
    catch (TimeoutException)
    {
        Log.Error("Sem resposta do serviço de mensagens em {Timeout}. Verifique se ele está rodando (make up).", replyTimeout);
        return 1;
    }

    Log.Information("Resposta recebida: {Reply}", replyJson);
    SubscriptionReply? reply = JsonSerializer.Deserialize<SubscriptionReply>(replyJson);

    if (reply is null)
    {
        Log.Error("Falha ao deserializar a resposta: {Reply}", replyJson);
        return 1;
    }

    if (!reply.Ok)
    {
        Log.Error("Erro na resposta: {Error}", reply.Error);
        return 1;
    }

    if (reply.Queue is null)
    {
        Log.Error("Resposta sem o nome da fila: {Reply}", replyJson);
        return 1;
    }
    string subscriberQueue = reply.Queue;

    Log.Information("Assinatura confirmada. Fila: {Queue}", subscriberQueue);

    // Consumidor dos tweets: ack manual, só depois de mostrar o tweet,
    // para o broker reentregar se o programa cair antes de terminar
    var tweetConsumer = new AsyncEventingBasicConsumer(channel);
    tweetConsumer.ReceivedAsync += async (sender, ea) =>
    {
        Tweet? tweet;

        try
        {
            tweet = JsonSerializer.Deserialize<Tweet>(ea.Body.Span);
        }
        catch (JsonException ex)
        {
            Log.Warning(ex, "Tweet inválido descartado: {Body}", Encoding.UTF8.GetString(ea.Body.Span));
            await channel.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false);
            return;
        }

        if (tweet is null)
        {
            Log.Warning("Mensagem vazia descartada em {Queue}", subscriberQueue);
            await channel.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false);
            return;
        }

        Console.WriteLine($"[{tweet.Topic ?? "?"}] @{tweet.Author ?? "desconhecido"}: {tweet.Text ?? "(sem texto)"}");

        await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
    };

    // Limita a quantidade de tweets entregues e ainda sem ack,
    // para o broker não empurrar a fila inteira para a memória do cliente de uma vez
    const ushort Prefetch = 10;
    await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: Prefetch, global: false);

    // A consumer tag identifica este consumidor no broker; é usada para cancelá-lo no fim
    string consumerTag = await channel.BasicConsumeAsync(
        queue: subscriberQueue,
        autoAck: false,
        consumer: tweetConsumer
    );

    Log.Information("Aguardando tweets em {Queue}...", subscriberQueue);

    var shutdown = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    Console.CancelKeyPress += (sender, e) =>
    {
        e.Cancel = true; // impede que o processo seja morto na hora; o encerramento é feito abaixo
        shutdown.TrySetResult();
    };

    Console.WriteLine("Pressione Ctrl+C para sair.");
    await shutdown.Task;

    // Para de receber tweets antes de fechar; os não entregues continuam guardados na fila
    await channel.BasicCancelAsync(consumerTag);
    Log.Information("Encerrando...");

    return 0;
}
finally
{
    Log.CloseAndFlush();
}


// Pergunta até receber uma resposta não vazia; retorna null se a entrada acabar (Ctrl+D)
static string? AskQuestion(string question)
{
    while (true)
    {
        Console.Write(question);
        var input = Console.ReadLine();
        if (input is null)
        {
            return null;
        }

        if (string.IsNullOrWhiteSpace(input))
        {
            Console.WriteLine("Resposta inválida. Tente novamente.");
            continue;
        }
        return input.Trim();
    }
}

// Separa os tópicos digitados por vírgula ou espaço, ignorando vazios
static string[] ParseTopics(string text)
{
    if (string.IsNullOrWhiteSpace(text))
    {
        return [];
    }
    return text.Split([',', ' '],
        StringSplitOptions.RemoveEmptyEntries |
        StringSplitOptions.TrimEntries);
}

// Pedido de assinatura enviado para tweets.control
record SubscriptionRequest(
    [property: JsonPropertyName("action")] string Action,
    [property: JsonPropertyName("subscriber")] string Subscriber,
    [property: JsonPropertyName("topics")] string[] Topics
);

// Resposta do serviço de mensagens ao pedido de assinatura
record SubscriptionReply(
    [property: JsonPropertyName("ok")] bool Ok,
    [property: JsonPropertyName("error")] string? Error,
    [property: JsonPropertyName("subscriber")] string? Subscriber,
    [property: JsonPropertyName("queue")] string? Queue,
    [property: JsonPropertyName("topics")] string[]? Topics
);

// Tweet recebido na fila do assinante (já com o tópico)
record Tweet(
    [property: JsonPropertyName("id")] string? Id,
    [property: JsonPropertyName("text")] string? Text,
    [property: JsonPropertyName("author")] string? Author,
    [property: JsonPropertyName("created_at")] string? CreatedAt,
    [property: JsonPropertyName("lang")] string? Lang,
    [property: JsonPropertyName("topic")] string? Topic
);
