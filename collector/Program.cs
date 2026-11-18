/*
   Data de criação: 17/11/2026
   Estudante: Caroline Lau
   Coletor de tweets: lê um arquivo JSON e publica os tweets no serviço de mensagens
*/

using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using System.Text.Json;
using System.Text.Json.Serialization;
using Serilog;

Log.Logger = new LoggerConfiguration()
    .WriteTo.Console()
    .CreateLogger();

try
{
    string path = args.Length > 0 ? args[0] : "tweets.json";
    Log.Information("Lendo tweets de {Path}", path);

    // Intervalo entre um tweet e outro
    var interval = TimeSpan.FromSeconds(1);

    // Lê o arquivo antes de conectar, para não publicar nada se ele estiver inválido
    List<Tweet>? tweets;

    try
    {
        await using var stream = File.OpenRead(path);
        tweets = await JsonSerializer.DeserializeAsync<List<Tweet>>(stream);
    }
    catch (FileNotFoundException)
    {
        Log.Error("Arquivo não encontrado: {Path}", path);
        return 1;
    }
    catch (JsonException ex)
    {
        Log.Error(ex, "JSON inválido em {Path}", path);
        return 1;
    }

    if (tweets is null)
    {
        Log.Error("O arquivo {Path} não contém uma lista de tweets", path);
        return 1;
    }

    if (tweets.Count == 0)
    {
        Log.Warning("Nenhum tweet em {Path}. Nada a publicar.", path);
        return 0;
    }

    Log.Information("{Count} tweets carregados", tweets.Count);

    // Conecta ao RabbitMQ
    string url = Environment.GetEnvironmentVariable("AMQP_URL") ?? "amqp://tweets:tweets@localhost:5672/";

    var factory = new ConnectionFactory
    {
        Uri = new Uri(url)
    };

    await using var connection = await factory.CreateConnectionAsync();
    Log.Information("Conectado ao broker");

    // Canal com confirmação: cada publicação espera o "recebi" do broker
    var channelOptions = new CreateChannelOptions(
        publisherConfirmationsEnabled: true,
        publisherConfirmationTrackingEnabled: true
    );

    await using var channel = await connection.CreateChannelAsync(channelOptions);
    Log.Information("Canal aberto");

    // Mensagem persistente (gravada em disco) com conteúdo JSON
    var properties = new BasicProperties
    {
        Persistent = true,
        ContentType = "application/json"
    };

    // Publica cada tweet no exchange tweets.ingest com a chave "raw"
    foreach (var tweet in tweets)
    {
        byte[] body = JsonSerializer.SerializeToUtf8Bytes(tweet);

        try
        {
            await channel.BasicPublishAsync(
                exchange: "tweets.ingest",
                routingKey: "raw",
                mandatory: true,
                basicProperties: properties,
                body: body
            );
        }
        // Tweet devolvido (sem fila para a chave "raw") ou recusado pelo broker
        catch (PublishException ex)
        {
            if (ex.IsReturn)
            {
                Log.Error("Tweet {Id} devolvido: nenhuma fila recebe a chave 'raw'. O serviço de mensagens está rodando (make up)?", tweet.Id);
            }
            else
            {
                Log.Error("Tweet {Id} recusado pelo RabbitMQ", tweet.Id);
            }
            return 1;
        }
        // O broker fechou o canal (por exemplo, o exchange não existe)
        catch (OperationInterruptedException ex)
        {
            Log.Error("O RabbitMQ fechou o canal ao publicar o tweet {Id}: {Motivo}", tweet.Id, ex.Message);
            return 1;
        }

        Log.Information("Tweet {Id} publicado", tweet.Id);

        // Espera antes do próximo tweet, para simular um fluxo chegando aos poucos
        await Task.Delay(interval);
    }

    Log.Information("{Count} tweets publicados", tweets.Count);

    return 0;
}
finally
{
    Log.CloseAndFlush();
}

// Tweet lido do arquivo (sem tópico: quem define é o classificador)
record Tweet(
    [property: JsonPropertyName("id")] string? Id,
    [property: JsonPropertyName("text")] string? Text,
    [property: JsonPropertyName("author")] string? Author,
    [property: JsonPropertyName("created_at")] string? CreatedAt,
    [property: JsonPropertyName("lang")] string? Lang
);

