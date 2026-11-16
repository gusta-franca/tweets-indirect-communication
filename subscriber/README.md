# Assinante (C#)

Aluna: Caroline Marques Lau

Cliente que assina um ou mais tópicos no serviço de mensagens e exibe os tweets recebidos.

## Como executar

Requer o .NET 10 e o RabbitMQ e o serviço de mensagens rodando (`make up` na raiz).

```bash
cd subscriber
dotnet run -- alice futebol voleibol   # nome e tópicos por argumento
dotnet run                             # ou o programa pergunta
```

Os tópicos podem ser separados por espaço ou vírgula. `Ctrl+C` encerra.
