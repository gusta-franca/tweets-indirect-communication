# Comunicação indireta de tweets

Alunos: Caroline Marques Lau, Gustavo Martins França e Maria Eduarda Bambini

Disciplina: Sistemas Distribuídos

Sistema publish/subscribe para distribuição de tweets por tópico, com comunicação indireta entre os módulos através de um serviço de mensagens com RabbitMQ.

## Arquitetura

```
Coletor(es) -> tweets.ingest -> tweets.raw -> 
Classificador(es) -> tweets.topics -> sub.<assinante> ->
Assinante
```

| Módulo               | Pasta              | Linguagem | Função                                                                 |
|----------------------|--------------------|-----------|------------------------------------------------------------------------|
| Coletor              | `collector/`       | C#        | Lê os tweets (arquivo JSON) e os envia para a fila de entrada          |
| Classificador        | `classifier/`      | Java      | Consome a fila de entrada, identifica o tópico e publica o tweet nele  |
| Assinante            | `subscriber/`      | C#        | Assina um ou mais tópicos e recebe os tweets deles                     |
| Serviço de mensagens | `message-service/` | Go        | Prepara o RabbitMQ e processa as assinaturas                           |

Os módulos podem ser escritos em qualquer linguagem: todos falam apenas **AMQP 0-9-1** com o RabbitMQ e não se comunicam diretamente.

## Requisitos

- Docker com Docker Compose (RabbitMQ e serviço de mensagens)
- Go (apenas para `make build-go` e `make run-mservice`)
- Java 17+ e Maven (classificador)
- .NET 10 (coletor e assinante)

## Como executar

Todos os comandos são executados na raiz do repositório.

| Comando                                                  | Descrição                                                              |
|----------------------------------------------------------|------------------------------------------------------------------------|
| `make all`                                               | Compila todos os módulos, sobe o RabbitMQ e o serviço e roda o classificador |
| `make up`                                                | Sobe o RabbitMQ e o serviço de mensagens                               |
| `make run-subscriber NAME=alice SUB_TOPICS="futebol voleibol"` | Roda um assinante (sem `NAME`/`SUB_TOPICS`, o programa pergunta) |
| `make run-classifier`                                    | Roda um classificador                                                  |
| `make run-collector`                                     | Publica os tweets de `collector/tweets.json` (outro arquivo: `FILE=outro.json`, relativo a `collector/`) |
| `make down`                                              | Para os containers, mantendo os dados                                  |
| `make clean`                                             | Para os containers e remove o volume com os dados do broker            |
| `make logs`                                              | Acompanha o log do serviço (`logs/message-service.log`)                |
| `make ps`                                                | Mostra o estado dos containers                                         |
| `make run-mservice`                                      | Roda o serviço localmente, RabbitMQ fica no docker                     |
| `make build-go` / `build-java` / `build-csharp`          | Compila só o módulo indicado                                           |

O RabbitMQ fica disponível em `amqp://tweets:tweets@localhost:5672/`.

**Ordem de inicialização:** serviço de mensagens -> assinantes -> classificadores -> coletor.

Os módulos esperam que as exchanges e filas já existam, e um assinante só recebe os tweets publicados depois da sua assinatura.

### Testando o sistema completo

Cada comando em um terminal, nesta ordem:

```bash
make up                                                     # 1. RabbitMQ + serviço de mensagens
make run-subscriber NAME=alice SUB_TOPICS=futebol           # 2. assinante de futebol
make run-subscriber NAME=bob SUB_TOPICS="voleibol futebol"  # 3. outro assinante (opcional)
make run-classifier                                         # 4. classificador (pode haver vários)
make run-collector                                          # 5. coletor
```

O coletor publica um tweet por segundo. Cada assinante deve exibir apenas os tweets dos tópicos que assinou, e o classificador registra os tweets descartados (sem tópico ou sem texto).

- **Assinante:** os tópicos podem ser separados por espaço ou vírgula. `Ctrl+C` encerra; os tweets publicados enquanto ele está fora continuam guardados na fila `sub.<nome>`.
- **Coletor:** publica cada tweet em `tweets.ingest` com confirmação do broker (*publisher confirms*) e termina ao chegar ao fim do arquivo.

## Configuração

Copie o arquivo `.env.example` para `.env` na raiz do repositório e ajuste os valores, se necessário:

| Variável    | Descrição                                                         | Padrão                                 |
|-------------|-------------------------------------------------------------------|----------------------------------------|
| `AMQP_URL`  | URL de conexão com o RabbitMQ                                     | `amqp://tweets:tweets@localhost:5672/` |
| `TOPICS`    | Tópicos aceitos nas assinaturas, separados por vírgula (opcional) | vazio (aceita qualquer tópico)         |
| `LOG_FILE`  | Caminho do arquivo de log                                         | `logs/message-service.log`             |

## Protocolo de Comunicação

A comunicação usa AMQP. As mensagens são JSONs em UTF-8. Quem consome deve confirmar cada mensagem somente depois de processá-la.

### Exchanges e filas

| Recurso            | Tipo             | Uso                                                              |
|--------------------|------------------|------------------------------------------------------------------|
| `tweets.ingest`    | exchange `direct`| O coletor publica aqui, com a chave de roteamento `raw`          |
| `tweets.raw`       | fila             | Recebe os tweets do coletor e os classificadores consomem         |
| `tweets.topics`    | exchange `direct`| O classificador publica aqui com a chave de roteamento = tópico |
| `tweets.control`   | fila             | Os assinantes enviam os pedidos de assinatura |
| `sub.<assinante>`  | fila             | Criada pelo serviço na assinatura. O assinante consome dessa fila      |

### Tweet

O coletor publica o tweet sem o campo `topic`, o classificador republica o mesmo JSON com `topic` igual à chave de roteamento.

```json
{
  "id": "1234567890",
  "text": "texto do tweet",
  "author": "usuario",
  "created_at": "2026-11-13T20:00:00Z",
  "lang": "pt",
  "topic": "futebol"
}
```

### Assinatura

O assinante publica em `tweets.control`:

```json
{ "action": "subscribe",   "subscriber": "alice", "topics": ["futebol", "voleibol"] }
{ "action": "unsubscribe", "subscriber": "alice", "topics": ["voleibol"] }
```

- O serviço cria a fila `sub.alice` (se não existir) e liga ou desliga cada tópico. Todos os tópicos assinados chegam na mesma fila, e o campo `topic` do tweet indica a origem.
- Nomes de assinante e de tópico são normalizados (somente letras minúsuculas).
- Se o pedido tiver `reply_to` (nome de uma fila), o serviço responde nela com `{"ok": true, "queue": "sub.alice", ...}` ou `{"ok": false, "error": "..."}`. O assinante só deve consumir `sub.<assinante>` depois dessa resposta.
- Para receber a resposta, o assinante deve declarar uma fila temporária (com nome gerado pelo broker) antes de publicar o pedido, informa o nome dela em `reply_to` e consome dela para ler a resposta.
