<div align="center">

# Kafka Conveyor

**Конвейер пакетной обработки сообщений для Apache Kafka на Go**

[![Go](https://img.shields.io/badge/go-1.21%2B-00ADD8?style=flat-square&logo=go&logoColor=white)](https://go.dev)
[![Tests](https://img.shields.io/badge/tests-9%20passing-3FB950?style=flat-square)](https://go.dev)
[![Deps](https://img.shields.io/badge/deps-none-555555?style=flat-square)](https://go.dev)

<p align="center">
  <em>Producer → бэтчинг → пул воркеров → Consumer → commit offset</em>
</p>

</div>

---

## Что это

Реализация конвейера (**conveyor / pipeline**) для потоковой обработки сообщений: входящие сообщения накапливаются в **батчи**, обрабатываются **пулом параллельных воркеров** с ретраями и подтверждаются продюсером через **cookie**.

Проект — абстракция над Apache Kafka: `Producer` и `Consumer` определены как интерфейсы, поэтому библиотека работает с любым брокером (или с заглушками в тестах) без прямой зависимости от `confluent-kafka-go` / `segmentio`.

## Архитектура

```
                            ┌─────────────────────────────┐
   input  ──▶ Producer.Next ─▶│  batcher (1 горутина)       │
   chan      (cookie)         │  • добирает до MaxBatchSize  │
   message                   │  • или ждёт BatchTimeout     │
                            └──────────┬──────────────────┘
                                       │ batchCh (буфер. WorkerCount×2)
                            ┌──────────▼──────────────────┐
                            │  worker pool (N горутин)     │
                            │  Consumer.Process(items)     │──▶ ретраи
                            │  Producer.Commit(cookie)     │──▶ ретраи
                            └─────────────────────────────┘
```

### Три стадии

| Стадия | Горутин | Задача |
| --- | --- | --- |
| **Batcher** | 1 | Набирает батч по размеру или по таймауту, присваивает `cookie` |
| **Workers** | `WorkerCount` | Обрабатывают батчи параллельно, с ретраями и backoff |
| **Control** | — | `context.Context` для отмены, `WaitGroup` для корректного завершения |

## Запуск

```bash
go test ./...        # 9 тестов, ~0.35s
go test -v ./...     # с подробным выводом
go vet ./...
```

Демо-точки входа нет — `main.go` пуст, проект используется как библиотека.

## Установка

```bash
go get github.com/rockbotum/kafka-conveyor
```

## Использование

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

err := conveyor.Pipe(
    ctx,
    kafkaProducer,   // реализует Producer: Next / Commit
    myConsumer,      // реализует Consumer: Process
    inputCh,         // <-chan message
    conveyor.Config{
        MaxBatchSize:    1000,               // сколько сообщений в батче
        BatchTimeout:    2 * time.Second,    // максимум ждать накопления
        MaxRetries:      3,                 // попыток повтора при ошибке
        RetryBackoff:    500 * time.Millisecond,
        WorkerCount:     8,                 // параллельных воркеров
        ShutdownTimeout: 10 * time.Second,
    },
)
```

## API

### `Pipe`

```go
func Pipe(ctx context.Context, p Producer, c Consumer, input <-chan message, cfg Config) error
```

Единственная точка входа. Блокируется до опустошения `input` или отмены `ctx`. Возвращает ошибку только при некорректных аргументах; ошибки обработки батчей пишутся в `batch.batchErr` и не прерывают конвейер.

### Интерфейсы

```go
type Producer interface {
    Next(ctx context.Context, input <-chan message) (message, bool, error)
    Commit(cookie int) error
}

type Consumer interface {
    Process(items []message) error
}
```

| Метод | Назначение |
| --- | --- |
| `Producer.Next` | Достать следующее сообщение. `ok == false` или ошибка → конвейер до-флашит остаток и завершится |
| `Producer.Commit` | Подтвердить батч по `cookie` (в Kafka это коммит оффсетов) |
| `Consumer.Process` | Обработать срез сообщений. Ошибка → ретрай с backoff |

### Конфигурация

| Поле | По умолчанию | Описание |
| --- | --- | --- |
| `MaxBatchSize` | `100` | Максимум сообщений в батче — батч уходит досрочно при достижении |
| `BatchTimeout` | `5s` | Максимальное время ожидания накопления батча |
| `MaxRetries` | `3` | Количество повторов при ошибке `Process` / `Commit` |
| `RetryBackoff` | `1s` | Пауза между попытками |
| `WorkerCount` | `5` | Размер пула воркеров |
| `ShutdownTimeout` | `10s` | Таймаут остановки |

> Любое невалидное значение (`≤ 0`) заменяет **весь** конфиг целиком на набор по умолчанию — частичного слияния со значениями пользователя не происходит.

## Гарантии и семантика

| Свойство | Поведение |
| --- | --- |
| Порядок внутри батча | сохраняется |
| Порядок батчей | **не гарантируется** — воркеры обрабатывают параллельно |
| Коммит | только после успешного `Process` |
| Ошибка после ретраев | батч пропускается, конвейер продолжает работу |
| Частичный батч при выходе | гарантированно до-флашится перед завершением |
| Паника в воркере | не перехватывается — уронит весь процесс |

## Тесты

Покрыты граничные случаи:

| Тест | Проверяет |
| --- | --- |
| `TestNext_ReadsMessage` | чтение сообщения |
| `TestNext_ClosedChannel` | закрытый канал → `io.EOF` |
| `TestNext_ContextCanceled` | отмена контекста |
| `TestProcess_EmptyBatch` | пустой батч → ошибка |
| `TestPipe_NilArgs` | nil-продюсер / nil-консьюмер |
| `TestPipe_ProcessesBatches` | разбиение на батчи, все элементы обработаны |
| `TestPipe_UsesDefaultConfigOnInvalidConfig` | подстановка дефолтов |
| `TestPipe_ContextCancel` | корректный выход по `cancel()` |
| `TestPipe_ClosesOnInputClose` | завершение при закрытии `input` |

## Известные ограничения

- **`ShutdownTimeout` объявлен, но не используется** — при отмене контекста конвейер ждёт завершения воркеров без таймаута. Воркер в длинном ретрае задержит остановку.
- **Типы `message` и `batch` не экспортируются** — реализовать `Producer` / `Consumer` можно только внутри пакета. Для внешнего использования нужно вынести типы в API.
- **Ретраи с `time.Sleep`** не учитывают `ctx` — отмена не прерывает паузу.
- **Нет дедлекта на коммит** — если `Process` прошёл, а `Commit` упал после всех ретраев, батч будет обработан повторно при ребалансировке. Нужен идемпотентный консьюмер.

## Возможные улучшения

- Экспорт типов `Message` / `Batch` для реального использования как библиотеки.
- `select` с `<-ctx.Done()` в ретраях вместо `time.Sleep`.
- Реализовать `ShutdownTimeout` через отдельную горутину-планировщик.
- Метрики: Prometheus-гистограммы на размер батча, latency, глубину `batchCh`.
- Вариант с ограничением параллелизма по ключу (partition), чтобы сохранить порядок внутри partition.
- Интеграционные тесты против `testcontainers` с реальным Kafka.

## Назначение

Учебный / reference-проект по конкурентному программированию на Go: бэтчинг, пул воркеров, отмена через контекст, синхронизация и корректное завершение горутин. Полезен как основа для собственного consumer pipeline.

## Лицензия

Свободное использование.

---

<div align="center">
  <sub>Go · fan-in + worker pool · без внешних зависимостей</sub>
</div>
