# Comunicação indireta de tweets

Alunos: Caroline Marques Lau, Gustavo Martins França e Maria Eduarda Bambini

Disciplina: Sistemas Distribuídos

Sistema publish/subscribe para distribuição de tweets por tópico, com comunicação indireta entre os módulos através de um serviço de mensagens com RabbitMQ.

## Arquitetura

```
Coletor -> tweets.ingest -> tweets.raw -> 
Classificador(es) -> tweets.topics -> sub.<assinante> ->
Assinante
```

| Módulo                | Função                                                                                   |
|-----------------------|------------------------------------------------------------------------------------------|
| Coletor               | Lê os tweets (arquivo JSON) e os envia para a fila de entrada                            |
| Classificador         | Consome a fila de entrada, identifica o tópico e publica o tweet nele |
| Assinante             | Assina um ou mais tópicos e recebe os tweets deles                                       |
| Serviço de mensagens | Prepara o RabbitMQ e processa as assinaturas |

Os módulos podem ser escritos em qualquer linguagem: todos falam apenas **AMQP 0-9-1** com o RabbitMQ e não se comunicam diretamente.

## Como executar

Na raiz do repositório:

```bash
make up
```

| Comando              | Descrição                                                        |
|----------------------|------------------------------------------------------------------|
| `make up`            | Sobe o RabbitMQ e o serviço de mensagens                         |
| `make down`          | Para os containers, mantendo os dados                            |
| `make clean`         | Para os containers e remove o volume com os dados do broker      |
| `make logs`          | Acompanha o log do serviço (`logs/message-service.log`)          |
| `make run-mservice`  | Roda o serviço localmente, RabbitMQ fica no docker          |

O RabbitMQ fica disponível em `amqp://tweets:tweets@localhost:5672/`.

**Ordem de inicialização:** serviço de mensagens -> assinantes -> classificadores -> coletor. 

Os módulos esperam que as exchanges e filas já existam, e um assinante só recebe os tweets publicados depois da sua assinatura.

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
- Para receber a resposta, o assinante declara uma fila temporária (com nome gerado pelo broker) antes de publicar o pedido, informa o nome dela em `reply_to` e consome dela para ler a resposta.
