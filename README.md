# kora-outbox

Transactional outbox для сервисов на [Kora](https://github.com/Tinkoff/kora) 1.x с Postgres. Вы записываете сообщение
в той же транзакции базы данных, что и бизнес-данные; relay внутри вашего сервиса доставляет его после коммита — в Kafka
или в ваш собственный обработчик. Сообщение больше не теряется, если между коммитом и отправкой сервис упал, Kafka была
недоступна или шёл передеплой, а откаченная транзакция не отправляет ничего.

> **Доставка at-least-once: каждый получатель обязан быть идемпотентным по id сообщения.** Одно и то же сообщение
> может прийти больше одного раза (падение между отправкой и коммитом relay, таймаут, повтор). Exactly-once из outbox
> в Kafka не даёт ни одна реализация: коммит в Postgres и запись в Kafka не атомарны. Effectively-once получается на
> стороне получателя: у каждого сообщения стабильный UUIDv7, в Kafka это заголовок `outbox-message-id` (потребитель на
> Kora отсеивает повторы через [inbox](#потребление-через-inbox)), для HTTP передавайте его как `Idempotency-Key`.

> **Требуется Kora 1.2.23 или новее.** Библиотека объявляет `api(platform("ru.tinkoff.kora:kora-parent:1.2.23"))`.
> В сборке Gradle её подключение поднимает все модули Kora в вашем runtime classpath до 1.2.23, а через
> `database-jdbc` — и HikariCP до 7.1.0. Выставьте свою платформу `kora-parent` и annotation processors
> (`annotation-processors` или `symbol-processors` для KSP) тоже на 1.2.23, чтобы сгенерированный код и runtime
> совпадали.

- Артефакты: `io.github.dsudomoin:kora-outbox` и `io.github.dsudomoin:kora-outbox-kafka`, `0.1.0-SNAPSHOT`
  (пока только через `mavenLocal()`).
- Байткод Java 17; работает из Java и Kotlin. Блокирующий API: `send` выполняется внутри `JdbcConnectionFactory.inTx`.

Общая схема: сообщение записывается одной транзакцией с бизнес-данными, relay на любом инстансе сервиса после коммита
передаёт его обработчику, а потребитель отбрасывает повторы через inbox.

```mermaid
flowchart TD
    tx["db.inTx: бизнес-изменения + outbox.send"]
    subgraph pg["Postgres"]
        direction LR
        biz[("бизнес-таблицы")]
        outbox[("outbox: партиции по xid8")]
    end
    tx -->|"одна транзакция"| pg
    outbox -->|"закоммиченные строки"| relay["relay на любом инстансе:<br/>тики, шарды, воркеры outbox-relay-N"]
    relay -->|handleBatch| handler["OutboxHandler: Kafka или свой"]
    handler -->|"заголовок outbox-message-id"| kafka[["Kafka"]]
    handler -.->|свой обработчик| other["HTTP API, другая БД, …"]
    kafka --> consumer["потребитель с Inbox:<br/>повторы по outbox-message-id<br/>отбрасываются в его scope"]
```

## Содержание

- [Гарантии](#гарантии)
- [Требования](#требования)
- [Установка](#установка)
- [Быстрый старт](#быстрый-старт): подключение, настройка, отправка, Kafka, свои обработчики, inbox
- [Как работает relay](#как-работает-relay)
- [Обработка сбоев](#обработка-сбоев)
- [Справочник по конфигурации](#справочник-по-конфигурации)
- [Требования к Kafka](#требования-к-kafka)
- [Эксплуатация](#эксплуатация): таблицы, партиции, dead letters, процедуры восстановления, деплои
- [Наблюдаемость](#наблюдаемость)
- [Тестирование вашего сервиса](#тестирование-вашего-сервиса)
- [Бенчмарки](#бенчмарки)
- [Заметки о дизайне](#заметки-о-дизайне)
- [Известные ограничения](#известные-ограничения)
- [Скиллы для AI-агентов](#скиллы-для-ai-агентов)

## Гарантии

### Гарантируется

- **Доставка at-least-once.** Каждое сообщение, чья транзакция закоммичена, передаётся обработчику, пока тот не
  сообщит о доставке, либо переносится в dead-таблицу с записью уровня ERROR в логе и метрикой
  `outbox.messages.failed{outcome="dead"}`. Это выполняется при падениях приложения, зависших воркерах, недоступности
  цели любой длительности, рестартах Postgres и передеплоях.
- **Откаченные сообщения никогда не доставляются.** Строка сообщения — часть вашей транзакции. `send` вне транзакции
  бросает `IllegalStateException`; в режиме autocommit он не выполняется никогда.
- **Доставка в порядке коммитов для каждой пары (topic, key).** Для двух сообщений с одним и тем же топиком и одним и
  тем же ненулевым ключом (сравнение побайтовое): если транзакция T1 закоммичена до того, как транзакция T2 отправила
  своё сообщение, обработчик получит сообщение T1 первым и не получит сообщение T2, пока сообщение T1 не доставлено.
  Сообщения одного ключа внутри одной транзакции сохраняют порядок вызовов `send` и `sendAll`. Исключений два:
  сообщение, перенесённое в dead-таблицу, больше не удерживает свой ключ, и ключ продолжает со следующего сообщения;
  сообщение, отложенное через [`Outcome.requeue()`](#свой-обработчик), доставляется после тех сообщений своего ключа,
  которые были доставлены, пока оно было отложено.
- **Стабильный id сообщения.** Каждое сообщение при отправке получает id в формате UUIDv7. `send` возвращает его, relay
  передаёт его при каждой попытке, а обработчик Kafka добавляет его в заголовок `outbox-message-id`. Потребители
  дедуплицируют по нему через [inbox](#потребление-через-inbox).
- **Длинные транзакции не задерживают доставку.** Транзакция, открытая минутами, задерживает только собственные
  сообщения (и писателей тех же ключей, которые ждут её блокировок по ключам). Сообщения, закоммиченные за это время,
  доставляются вовремя (см. [Заметки о дизайне](#заметки-о-дизайне)).
- **Изоляция.** Сбойная пара (topic, key) блокирует только себя. Недоступная цель приостанавливает доставку и ничего не
  теряет. Одно плохое сообщение не может застопорить шард: его изолируют и откладывают в сторону.

### Не гарантируется

- **Exactly-once.** Сообщение может быть доставлено больше одного раза: после падения между отправкой и коммитом relay,
  после таймаута Kafka для записи, которая на самом деле записалась, или после вызова обработчика, прерванного по
  дедлайну. Для effectively-once обработки используйте inbox; его дедупликация работает в пределах scope потребителя и
  ограничена его временным окном.
- **Порядок между ключами.** У разных пар (topic, key), разных топиков и сообщений без ключа нет взаимного порядка.
  Сообщения без ключа не упорядочены и между собой.
- **Порядок вокруг dead letter.** Когда сообщение ушло в dead letters, следующие сообщения его ключа идут дальше.
  Переотправленный dead letter или строка, возвращённая из осиротевшей партиции, — это новое сообщение, и оно идёт после
  них.
- **То, что теряет или переупорядочивает сама Kafka.** `acks=all` надёжен ровно настолько, насколько надёжен топик, а
  добавление партиций переносит ключи; см. [Требования к Kafka](#требования-к-kafka).
- **Асинхронная репликация.** После failover на реплику, где нет части коммитов, сообщения этих коммитов могут быть уже
  доставлены (фантомы). Их бизнес-данные тоже потеряны. Если это важно, используйте синхронную репликацию.
- **Верхняя граница задержки.** Когда цель здорова, доставка быстрая (десятки миллисекунд), но во время сбоев, backoff
  (`relay.retry.maxDelay`) или долгих вызовов обработчика верхней границы нет.
- **Базы с несколькими пишущими узлами.** Блокировки шардов, тики и курсоры живут в primary той же базы, что и
  бизнес-данные; это и есть точка синхронизации всех инстансов, в каком бы ЦОДе они ни работали. Мульти-мастер и
  распределённые SQL-базы, где одну таблицу пишут несколько узлов, не поддерживаются, а split-brain двух primary ломает
  и бизнес-данные, и outbox. Данные, разнесённые по независимым кластерам Postgres (шардирование по базам), — это
  независимый outbox в каждой базе; порядок ключа сохраняется, если все его данные пишутся в одну базу.
- **Несколько независимых relay для одной таблицы.** У одной таблицы outbox один набор курсоров и один обработчик на
  граф приложения. Маршрутизируйте по топику внутри обработчика ([`byTopic`](#свой-обработчик)).

Куда попадает сообщение: шард зависит только от пары (topic, key) и для ключа не меняется, физическая партиция outbox —
только от xid транзакции-писателя (то есть от времени записи), а партицию Kafka по ключу выбирает партиционер Kafka.
Сообщение без ключа попадает в случайный шард.

```mermaid
flowchart LR
    send["outbox.send(topic, key, …)<br/>в транзакции с xid X"]
    send -->|"hash(topic, key)"| shard["шард: hash mod outbox.shards,<br/>у ключа всегда один и тот же"]
    send -->|"xid X"| part["партиция outbox_x…:<br/>диапазон xid, куда попал X,<br/>то есть по времени записи"]
    send -->|key| kp["партиция топика Kafka:<br/>партиционер Kafka по key"]
```

## Требования

- **Java 17** или новее.
- **Kora 1.2.23** или новее (см. примечание в начале).
- **Postgres 16** или новее. Тесты и бенчмарки запускаются на Postgres 16.
- **Один primary.** Outbox лежит в той же базе, что и бизнес-данные, а relay подключается к тому же primary, что и
  писатели (на read-only реплике шаг не закоммитится). Привязывать инстансы к шардам или к ЦОДу не нужно: инстанс в
  дальнем ЦОДе платит сетевую задержку на каждом запросе шага, но не корректностью.
- Клиент **Kafka** 3.9.2 (эту версию использует Kora 1.2.23) для `kora-outbox-kafka`. Настройки брокера — в разделе
  [Требования к Kafka](#требования-к-kafka).

### Роль в базе и привилегии

Библиотека сама создаёт и поддерживает свои таблицы (миграции в комплект не входят). Запускайте её под ролью, которая:

- имеет `CREATE` на схеме таблиц. Bootstrap выполняется на каждом инстансе, использует `CREATE TABLE IF NOT EXISTS`
  и требует этого права, даже когда таблицы уже существуют;
- владеет таблицами (или входит в роль-владельца). Обслуживание партиций выполняет `CREATE TABLE`,
  `ALTER TABLE ... ATTACH/DETACH PARTITION` и `DROP TABLE`, а для них нужно владение. Если таблицы создаёт роль
  миграций, сделайте роль приложения членом этой роли;
- может делать `SELECT` из identity-последовательности `<base>_id_seq` (владелец может). Каждый тик читает её последнее
  значение;
- входит в `pg_read_all_stats` (или `pg_monitor`): рекомендуется. Без этого Postgres скрывает, как долго идут
  транзакции сессий, вошедших под другими ролями. Тогда перед detach хвоста партиций обслуживание дважды, с интервалом
  20 с, проверяет их блокировки на outbox, а их снапшоты замечает только у pg_dump. Если членства нет, bootstrap пишет
  WARN. Это касается и сессий вашего собственного приложения, если они переключают роль (`SET ROLE`, отдельные учётные
  данные на инстанс).

Не задавайте роли `statement_timeout` в несколько секунд. DDL партиций и bootstrap выставляют собственные таймауты, но
шаги очистки при обслуживании выполняются с настройкой роли по умолчанию и повторяются на каждом запуске, если она их
обрывает. Оставьте `track_activities` включённым (так по умолчанию); сессии без него считаются скрытыми, как описано
выше.

Библиотека берёт двухаргументные advisory-блокировки (`pg_advisory_xact_lock(ns, key)`) в двух пространствах имён,
производных от имени таблицы. Ваши собственные двухаргументные advisory-блокировки могут с ними совпасть; блокировки с
одним аргументом `bigint` — никогда.

### Соединения: без transaction pooling

**Bootstrap и обслуживанию нужно прямое соединение или пулер в сессионном режиме.** Они держат сессионные
advisory-блокировки и выставляют сессионные настройки вокруг DDL в режиме autocommit. За прокси с transaction pooling
(pgbouncer или Odyssey в режиме transaction) всё это утекало бы в сессии других клиентов.

- Библиотека проверяет соединение: три последовательных запроса должны выполниться в одной серверной сессии
  (`pg_backend_pid()`). Если это не так, bootstrap валит старт. Обслуживание вместо этого пропускает запуск, пишет ERROR
  не чаще раза в минуту и выставляет gauge `outbox.maintenance.transaction_pooling` (или
  `outbox.inbox.maintenance.transaction_pooling`) в 1.
- Проверка — best effort. pgbouncer переиспользует серверные соединения в порядке LIFO, поэтому спокойный пул часто
  проходит проверку при старте, а проблема проявляется в обслуживании под нагрузкой. Не рассчитывайте на то, что старт
  упадёт.
- `send` и relay используют только состояние уровня транзакции (`SET LOCAL`, транзакционные advisory-блокировки). Но
  библиотека использует для всего единственную `JdbcConnectionFactory` вашего приложения, поэтому сервис, чей
  DataSource обязан ходить через transaction pooler, сегодня не может её использовать.

### Размер пула соединений

Прибавьте `relay.workers + 2` к `db.maxPoolSize`, плюс 1 на время работы `OutboxRelay.drain()`: каждый relay-воркер
держит соединение, пока работает, а обслуживание outbox и обслуживание inbox берут по одному на каждый запуск. Писатели
используют соединение вашей собственной транзакции.

### `max_locks_per_transaction` для массовой отправки

Сообщение с ключом берёт транзакционную advisory-блокировку на свою пару (topic, key) и держит её до коммита. Именно
эта блокировка делает порядок по ключу равным порядку коммитов. Поэтому `sendAll` с N различными ключами держит
одновременно N блокировок — как и транзакция, которая вызывает `send` для N различных ключей.

Advisory-блокировки живут в общей таблице блокировок, которая вмещает
`max_locks_per_transaction × (max_connections + max_prepared_transactions)` блокировок на все сессии вместе.
Рассчитайте её на блокировки, которые ваши параллельные массовые транзакции держат в пике, плюс обычные блокировки
всего остального. Иначе транзакции падают с `out of shared memory` (SQLState 53200, hint: increase
`max_locks_per_transaction`).

Пример: два писателя, каждый из которых коммитит батчи `sendAll` по 10 000 различных ключей, держат до 20 000
блокировок. Значения по умолчанию дают 64 × (100 + 0) = 6 400 слотов, этого мало. Бенчмарки запускаются с
`max_locks_per_transaction = 256` и `max_connections = 300`: 76 800 слотов. Настройка требует рестарта, а у hot
standby значение должно быть не меньше, чем у его primary.

## Установка

Соберите и опубликуйте библиотеку в локальный Maven-репозиторий из этого репозитория (сборка использует toolchain
Java 17, он должен быть установлен локально):

```bash
./gradlew publishToMavenLocal
```

Затем в вашем сервисе (Gradle Kotlin DSL):

```kotlin
repositories {
    mavenLocal()
    mavenCentral()
}

dependencies {
    implementation("io.github.dsudomoin:kora-outbox:0.1.0-SNAPSHOT")
    implementation("io.github.dsudomoin:kora-outbox-kafka:0.1.0-SNAPSHOT") // обработчик Kafka; без Kafka не нужен
}
```

`kora-outbox` приносит `database-jdbc`, `config-common`, `opentelemetry-common` и `micrometer-core` (1.12.13).
`kora-outbox-kafka` добавляет `kafka-clients` 3.9.2.

Двум опциональным модулям нужна зависимость, которую библиотека не приносит (она только компилируется с ней). Без неё
`@KoraApp`, включающий такой модуль, не скомпилируется или не стартует (`NoClassDefFoundError`).

| Модуль | Что нужно в вашем classpath |
|---|---|
| `io.github.dsudomoin.kora.outbox.json.OutboxJsonModule` | `ru.tinkoff.kora:json-common` (его приносит любой JSON-модуль Kora, например `json-module`) |
| `io.github.dsudomoin.kora.outbox.protobuf.OutboxProtobufModule` | `com.google.protobuf:protobuf-java` 3.x или новее (его приносят классы, сгенерированные protobuf) |

## Быстрый старт

### Подключение модулей

```java
@KoraApp
public interface App extends HoconConfigModule, JdbcDatabaseModule, JsonModule,
        OutboxModule,       // Outbox, TypedOutbox<T>, relay
        OutboxJsonModule,   // TypedOutbox<T> для классов с @Json
        KafkaOutboxModule,  // обработчик по умолчанию: relay в Kafka
        InboxModule {}      // Inbox для дедупликации на стороне потребителя
```

```kotlin
@KoraApp
interface App : HoconConfigModule, JdbcDatabaseModule, JsonModule,
    OutboxModule, OutboxJsonModule, KafkaOutboxModule, InboxModule
```

- `OutboxModule` требует ровно один `OutboxHandler` в графе. `KafkaOutboxModule` даёт обработчик по умолчанию; ваш
  собственный `OutboxHandler` его заменяет ([Свой обработчик](#свой-обработчик)).
- `InboxModule` без `OutboxModule` даёт сервису, который только потребляет, `Inbox` без обработчика и relay.
- Метрики включаются, когда в графе есть Micrometer `MeterRegistry` (`MetricsModule` из Kora). Трассировка включается,
  когда в нём есть OpenTelemetry `Tracer` (`OpentelemetryTracingModule` из Kora).
- Как всегда в Kora, компонент, от которого не зависит ни один root, не попадает в граф. Relay сам является root.

### Настройка

```hocon
db {
  jdbcUrl = "jdbc:postgresql://localhost:5432/app"
  username = "app"
  password = ${DB_PASSWORD}
  poolName = "app"
  maxPoolSize = 24 # ваша собственная нагрузка + outbox.relay.workers + 2
}

outbox {
  table = "app.outbox" # необязательно (по умолчанию "outbox"); указывайте схему, см. «Rolling-деплои»
  kafka.driverProperties {
    "bootstrap.servers" = "kafka-1:9092,kafka-2:9092,kafka-3:9092"
  }
}
```

У всех остальных ключей есть значения по умолчанию ([Справочник по конфигурации](#справочник-по-конфигурации)). При
первом старте библиотека создаёт свои таблицы, заранее создаёт партиции и запускает relay.

### Отправка в транзакции

`Outbox.send` должен выполняться внутри `JdbcConnectionFactory.inTx(...)`, в той же транзакции, что и ваши
бизнес-записи. Он возвращает id сообщения.

```java
@Component
public final class Orders {

    private final JdbcConnectionFactory db;
    private final OrderRepository repository;
    private final Outbox outbox;

    public Orders(JdbcConnectionFactory db, OrderRepository repository, Outbox outbox) {
        this.db = db;
        this.repository = repository;
        this.outbox = outbox;
    }

    public void create(Order order, byte[] event) {
        db.inTx(() -> {
            repository.insert(order);
            outbox.send(OutboxMessage.of("orders", order.userId(), event)); // ключ = id пользователя
        });
    }
}
```

```kotlin
@Component
class Orders(
    private val db: JdbcConnectionFactory,
    private val repository: OrderRepository,
    private val outbox: Outbox,
) {
    fun create(order: Order, event: ByteArray) = db.inTx<Unit> { _: Connection ->
        repository.insert(order)
        outbox.send(OutboxMessage.of("orders", order.userId, event))
    }
}
```

В Kotlin передавайте `inTx` аргумент типа и параметр `Connection`, как выше: у `JdbcConnectionFactory.inTx` четыре
перегрузки.

`OutboxMessage`:

- `OutboxMessage.of(topic, key, payload)`: `key` — это `String` (кодируется в UTF-8) или `byte[]`. Это ключ порядка
  и, для Kafka, ключ записи. Не больше 1024 байт.
- `OutboxMessage.of(topic, payload)`: сообщение без ключа. Сообщения без ключа распределяются по шардам случайно и не
  упорядочены. `of(topic, null, payload)` не компилируется (перегрузки для `String` и `byte[]` неоднозначны);
  используйте эту.
- `payload` может быть `null` — это tombstone в Kafka. `topic` обязателен, не длиннее 249 символов.
- `message.withHeader(name, value)` добавляет заголовок; заголовки сохраняют порядок и могут повторяться, как в Kafka.
- `new OutboxMessage(topic, key, payload, headers)` — полная форма.

Другие способы отправки:

- **`sendAll(List<OutboxMessage>)`**: два запроса при любом размере, id возвращаются в порядке входного списка. Он
  блокирует все свои ключи разом, в фиксированном порядке, поэтому параллельные транзакции `sendAll` никогда не
  попадают на них в deadlock. **Безопасное правило:** отправляйте все сообщения с ключом одной транзакции одним
  `sendAll`, без других `send` в этой транзакции. Транзакция, которая уже держит блокировки ключей от предыдущего
  `send` или `sendAll`, может попасть в deadlock с другой; тогда Postgres прерывает одну из них (SQLState 40P01), и вы
  её повторяете.
- **`send(Connection, OutboxMessage)`** — для кода, который сам управляет соединением. Оно должно быть в транзакции
  (autocommit выключен). Сразу после коммита relay будит только текущее соединение `inTx` из Kora; с любым другим
  соединением доставка ждёт idle-опроса relay (`relay.idlePollInterval`).
- **Контекст трассировки.** `send` сохраняет текущую трассу (`traceparent`, `tracestate`) в заголовках сообщения, если
  сообщение ещё не несёт `traceparent`.

`inTx` возвращает то, что возвращает лямбда:

```java
UUID id = db.inTx(connection -> {
    repository.insert(order);
    return outbox.send(OutboxMessage.of("orders", order.userId(), event));
});
```

#### Типизированные сообщения: `TypedOutbox<T>`

`OutboxModule` предоставляет `TypedOutbox<T>` для каждого `T`, у которого есть компонент `OutboxSerializer<T>`.
`OutboxJsonModule` выводит такой сериализатор для каждого класса с `JsonWriter` из Kora (классы с `@Json`).

```java
@Json
public record OrderCreated(long id, String userId) {}

@Component
public final class Orders {

    private final JdbcConnectionFactory db;
    private final TypedOutbox<OrderCreated> events;

    public Orders(JdbcConnectionFactory db, TypedOutbox<OrderCreated> events) {
        this.db = db;
        this.events = events;
    }

    public void create(Order order) {
        db.inTx(() -> {
            // вставка заказа ...
            events.send("orders", order.userId(), new OrderCreated(order.id(), order.userId()));
        });
    }
}
```

```kotlin
@Json
data class OrderCreated(val id: Long, val userId: String)

@Component
class Orders(private val db: JdbcConnectionFactory, private val events: TypedOutbox<OrderCreated>) {
    fun create(order: Order) = db.inTx<Unit> { _: Connection ->
        // вставка заказа ...
        events.send("orders", order.userId, OrderCreated(order.id, order.userId))
    }
}
```

- **Явный сериализатор важнее JSON-шаблона.** Ваш собственный компонент `OutboxSerializer<T>` используется для своего
  `T`, даже если `T` помечен `@Json` и подключён `OutboxJsonModule`:

  ```java
  default OutboxSerializer<OrderCreated> orderCreatedSerializer() {
      return (topic, value) -> Avro.encode(value); // любой формат: Avro, CSV, ...
  }
  ```

- **Четырёхаргументному `send` без ключа нужно приведение типа:** `send(topic, key, value, headers)` существует для
  ключа `String` и для ключа `byte[]`, поэтому одиночный `null` неоднозначен:

  ```java
  events.send("orders", (String) null, value, List.of(new OutboxHeader("source", sourceBytes)));
  ```

- Значение `null` отправляется как payload `null` (tombstone). Сериализатор не должен возвращать `null`: в этом случае
  `TypedOutbox` бросает `OutboxSerializationException`, а не отправляет случайный tombstone, который удалил бы ключ в
  compacted-топике. Сериализатор, бросивший исключение, приводит к тому же исключению. В обоих случаях ничего не
  записывается, и что делать дальше, решает ваша транзакция.

#### Protobuf: `ProtobufOutbox`

Добавьте `OutboxProtobufModule` в `@KoraApp`. Один компонент `ProtobufOutbox` отправляет любое protobuf-сообщение в его
бинарной кодировке (`OrderEvent` здесь — класс, сгенерированный protobuf):

```java
db.inTx(() -> {
    // вставка заказа ...
    protobufOutbox.send("orders", order.userId(), OrderEvent.newBuilder().setId(order.id()).build());
});
```

Сообщение `null` — это tombstone, как и в `TypedOutbox`.

### Доставка в Kafka

С `KafkaOutboxModule` и заданным `outbox.kafka.driverProperties."bootstrap.servers"` relay отправляет каждое сообщение
в топик Kafka, имя которого указано в его `topic`:

| Запись Kafka | Значение |
|---|---|
| topic | `topic` сообщения |
| key | `key` сообщения (`null` для сообщений без ключа) |
| value | `payload` (`null` — это tombstone) |
| headers | заголовки сообщения по порядку (включая `traceparent` и `tracestate`), затем `outbox-message-id` (UUID строкой в UTF-8) |
| partition | партиционер Kafka по умолчанию (записи с ключом — по хешу ключа). Пока партиция топика остывает после сбоев, отправитель на каждый вызов выбирает для записей этого топика без ключа одну здоровую партицию |
| timestamp | на усмотрение Kafka (время отправки) |

Продюсер идемпотентный, с `acks=all`, и закрывается при первой неудачной отправке, чтобы ни одна более поздняя запись
сбойного ключа не могла её обогнать. В разделе [Требования к Kafka](#требования-к-kafka) перечислены настройки
продюсера, которые библиотека навязывает, отвергает и задаёт по умолчанию, и настройки топиков, от которых зависят
гарантии.

`KafkaOutboxPublisher<K, V>` отправляет в один топик через `Serializer`-ы Kafka (включая `@Json`-сериализаторы Kora).
Ключ порядка — сериализованный ключ Kafka:

```java
var publisher = new KafkaOutboxPublisher<>(outbox, "orders", keySerializer, valueSerializer);
db.inTx(() -> {
    publisher.send(order.userId(), event);
});
```

Он отвергает некорректное имя топика при создании, а также сериализатор, который возвращает `null` для ненулевого
ключа или значения.

### Свой обработчик

Обработчик доставляет записи куда-то ещё: в HTTP API, в другую базу, в несколько целей. Реализуйте `OutboxHandler`:

```java
public interface OutboxHandler {
    void handle(OutboxRecord record) throws Exception;          // одна запись
    default List<Outcome> handleBatch(List<OutboxRecord> records); // по одному Outcome на запись, в том же порядке
    static boolean windDownRequested();
    static OutboxHandler byTopic(Map<String, OutboxHandler> routes, OutboxHandler fallback);
}
```

`OutboxRecord` несёт `id`, `messageId`, `topic`, `key`, `payload`, `headers`, `createdAt`, `attempts` (число
предыдущих неудачных попыток) и `requeues` (сколько раз запись уже откладывали через `Outcome.requeue()`).

**`handle`: простая форма.** Доставьте одну запись и верните управление. Для записи, которая никогда не сможет пройти,
бросьте `OutboxPermanentException`: она переносится в dead-таблицу, а её ключ продолжает со следующего сообщения.
Любое другое исключение, включая `Error`, означает «повторить позже». `handleBatch` по умолчанию вызывает `handle` для
каждой записи по порядку:

- после сбоя пара (topic, key) этой записи пропускается до конца вызова (`NotAttempted`); записи без ключа никогда не
  блокируют друг друга;
- пока ничего не доставлено, он останавливается после сбоев на 3 различных ключах, так как цель, похоже, недоступна, и
  возвращает `NotAttempted` для остальных;
- перед каждой записью он проверяет `OutboxHandler.windDownRequested()` и останавливается, если сворачивание запрошено
  (см. дедлайны ниже);
- каждый вызов `handle` выполняется в контексте Kora, несущем трассу записи, поэтому клиенты Kora, вызванные из него,
  продолжают эту трассу.

HTTP-цель с `@HttpClient` из Kora:

```java
@HttpClient(configPath = "httpClient.audit")
public interface AuditClient {
    @HttpRoute(method = HttpMethod.POST, path = "/events")
    void post(@Header("Idempotency-Key") String messageId, byte[] body);
}

@Component
public final class AuditHandler implements OutboxHandler {

    private final AuditClient client;

    public AuditHandler(AuditClient client) {
        this.client = client;
    }

    @Override
    public void handle(OutboxRecord record) {
        try {
            client.post(record.messageId().toString(), record.payload());
        } catch (HttpClientResponseException e) {
            if (e.getCode() >= 400 && e.getCode() < 500 && e.getCode() != 408 && e.getCode() != 429) {
                throw new OutboxPermanentException("audit rejected " + record.messageId(), e);
            }
            throw e; // 5xx, 408, 429: повтор с backoff
        }
    }
}
```

```hocon
httpClient.audit {
  url = "https://audit.internal"
  requestTimeout = 5s # таймаут чтения в Kora по умолчанию 2 мин: намного больше outbox.relay.batchTimeout
}
```

**`handleBatch`: быстрая форма.** Переопределите его, чтобы доставлять много записей за один round trip, как это
делает обработчик Kafka. Контракт:

- Верните по одному `Outcome` на запись, в том же порядке: `Outcome.delivered()`, `Outcome.retry(error)`,
  `Outcome.retry(error, notBefore)`, `Outcome.permanent(error)`, `Outcome.notAttempted()`,
  `Outcome.notAttempted(notBefore)`, `Outcome.requeue()` или `Outcome.requeue(notBefore)` (см. ниже). `notBefore`
  ограничен 24 ч. `NotAttempted` не тратит попытку записи.
- Никогда не доставляйте запись, чья пара (topic, key) уже дала сбой раньше в этом же вызове.
- **Сообщайте о сбоях через outcome, а не исключениями.** Вызов, который бросил исключение (включая `Error`) или
  вернул некорректный список (null, неверный размер, элемент null), проваливает все свои записи как `Retry`, в том
  числе уже доставленные, и они доставляются повторно. Затем relay приостанавливает шард и уменьшает вдвое число
  записей, передаваемых за вызов, пока сбойная запись не останется одна, и отводит её. В dead letters запись попадает
  только из вызова с одной этой записью, который бросил `OutboxPermanentException`. Исключения и некорректные списки
  пишутся в лог с уровнем ERROR.

**Отложить запись: `Outcome.requeue()`.** Нужен обработчику, который ведёт по ключу очередь задач (ключ — id счёта),
когда запись пока нельзя обработать: идёт другой процесс того же счёта, и он закончится сам. `Outcome.retry` тут не
подходит: он блокирует ключ, и остальные события счёта ждут за этой записью. `requeue` значит «не сейчас: пропусти
дальше остальные записи моего ключа и верни меня позже»:

- В той же транзакции, что и шаг relay, запись переносится в `<base>_parked` с тем же `message_id`, топиком, ключом,
  payload, заголовками и `created_at`. Откаченный шаг ничего не переносит, и запись приходит снова: ни потери, ни
  лишней копии.
- Отложенная запись не блокирует ключ: пока она ждёт, следующие записи ключа доставляются.
- Обработчик снова получает её при первом из двух событий. Первое — следующая новая запись её ключа: отложенные
  записи ключа идут в том же вызове перед ней, в исходном порядке. Второе — наступил её срок: через
  `outbox.relay.requeue.delay` (по умолчанию 1 с), для `requeue(notBefore)` — в `max(now + delay, notBefore)`. Запись
  без ключа возвращается только по сроку; запись, чей ключ ждёт в retry-таблице, — тоже, пока он там.
- Это не сбой: попытка не тратится, шард не встаёт на паузу, backoff и число записей на вызов не меняются. Каждый
  `requeue` прибавляет 1 к `record.requeues()`, и ответ `requeue` на запись, подмешанную перед новой записью ключа,
  тоже. `requeue` сверх `outbox.relay.requeue.maxRequeues` (по умолчанию 0 — без предела) переносит запись в
  dead-таблицу с ошибкой «requeue limit reached».
- Outcome вернувшейся записи: `Delivered` убирает её из `<base>_parked`; `Requeue` и `NotAttempted` оставляют там
  (срок не раньше `now + delay`); `Permanent` переносит в dead-таблицу; `Retry` делает её головой ключа в
  retry-таблице, как любую сбойную запись (и `maxAttempts` действует так же): с этого момента ключ заблокирован, а
  более поздние отложенные записи ключа встают за ней.
- Вызов с чанком несёт сверх него до лимита чанка (не больше `batchSize`) отложенных записей ключей чанка, самые
  старые первыми: всего до `2 × batchSize` записей.
- `requeue` головы из retry-таблицы переносит её в `<base>_parked`, и ключ продолжает со следующей записи. `requeue`
  записи, чей ключ уже дал сбой раньше в этом вызове, не откладывает её: она встаёт в retry-таблицу за сбойной
  записью. Вызов, после которого шард встаёт на паузу, ничего не откладывает: эти записи придут снова.
- `handleBatch` по умолчанию никогда не возвращает `requeue`: обработчик, которому он нужен, переопределяет
  `handleBatch`. `byTopic` передаёт `requeue` маршрута дальше, как любой outcome.

```java
@Component
public final class AccountJobsHandler implements OutboxHandler {

    private static final int GIVE_UP_AFTER = 1000; // отложена столько раз: в dead letters

    private final AccountProcesses processes; // какие процессы счетов идут сейчас
    private final AccountJobs jobs;

    public AccountJobsHandler(AccountProcesses processes, AccountJobs jobs) {
        this.processes = processes;
        this.jobs = jobs;
    }

    @Override
    public void handle(OutboxRecord record) throws Exception {
        jobs.start(record);
    }

    @Override
    public List<Outcome> handleBatch(List<OutboxRecord> records) { // один топик, ключ = id счёта
        var outcomes = new ArrayList<Outcome>(records.size());
        var failedKeys = new HashSet<ByteBuffer>();
        for (var record : records) {
            var key = ByteBuffer.wrap(record.key());
            if (failedKeys.contains(key) || OutboxHandler.windDownRequested()) {
                outcomes.add(Outcome.notAttempted());
            } else if (processes.blocks(record)) { // идёт процесс того же счёта, который мешает этому событию
                outcomes.add(record.requeues() < GIVE_UP_AFTER
                        ? Outcome.requeue()
                        : Outcome.permanent(new IllegalStateException("blocked " + record.requeues() + " times")));
            } else {
                try {
                    handle(record);
                    outcomes.add(Outcome.delivered());
                } catch (Exception e) {
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    outcomes.add(Outcome.retry(e));
                    failedKeys.add(key);
                }
            }
        }
        return outcomes;
    }
}
```

Тот же предел без кода — `outbox.relay.requeue.maxRequeues`; в обоих случаях запись уходит в dead letters.

**Дедлайны.** Каждый вызов выполняется на потоке обработчика relay и ограничен `outbox.relay.batchTimeout` (по
умолчанию 30 с):

- На 80 % от него (24 с) relay просит вызов свернуться: `OutboxHandler.windDownRequested()` становится true, а поток
  прерывается. С этого момента не начинайте новые записи и верните `NotAttempted` для остальных, как делает реализация
  по умолчанию. Relay сохраняет фактические outcome и перечитывает непопробованные записи на следующем шаге. Вызов,
  который только отложил (или не попробовал) подмешанные отложенные записи и не попробовал ни одной новой,
  проваливается целиком, как вызов, где не попробовано ничего: пауза шарда, вдвое меньший чанк, проба, отвод ключа.
- Проверяйте `windDownRequested()`, а не только флаг прерывания: некоторые клиенты сбрасывают флаг (блокирующий
  HTTP-клиент Kora на транспорте JDK оборачивает `InterruptedException`, не восстанавливая флаг).
- От вызова, который всё ещё выполняется по истечении `batchTimeout`, relay отказывается, и все его записи
  повторяются, включая доставленные.
- Поэтому держите таймауты на одну запись (таймауты HTTP-запросов, собственные таймауты каждого маршрута `byTopic`)
  заметно ниже `batchTimeout`, а `batchSize ×` время на запись — тоже ниже него (если обработчик откладывает записи,
  вызов несёт до `2 × batchSize`).
- Вызов, который игнорирует прерывания и переживает `batchTimeout`, удерживает свой поток. У каждого инстанса
  `max(4, 2 × workers)` потоков обработчика. Когда заняты все потоки инстанса, этот инстанс пропускает свои шарды с
  растущим backoff, пока эти вызовы не вернутся; другие инстансы забирают шарды через `relay.retry.initialDelay`.

**Несколько целей: `byTopic`.** Один обработчик на граф; маршрутизируйте внутри него. С `KafkaOutboxModule` фабрика,
объявленная ровно как `OutboxHandler`, заменяет обработчик Kafka по умолчанию:

```java
@KoraApp
public interface App extends HoconConfigModule, JdbcDatabaseModule, JdkHttpClientModule, OutboxModule,
        KafkaOutboxModule {

    default OutboxHandler outboxHandler(KafkaOutboxSender kafka, AuditClient audit) {
        return OutboxHandler.byTopic(Map.of("audit", new AuditHandler(audit)), kafka.handler());
    }
}
```

- `byTopic` передаёт каждому маршруту его записи в исходном порядке и расставляет outcome обратно по местам. Маршрут,
  который бросил исключение или вернул некорректный список, получает `Retry` только для своих записей; `Error` проваливает
  весь вызов. Маршруты выполняются один за другим, поэтому маршрут, игнорирующий сворачивание, держит весь вызов до
  `batchTimeout`.
- Пусть это будет единственный компонент `OutboxHandler` без тега. Создавайте вложенные обработчики внутри фабрики, как
  выше (тогда `AuditHandler` сам не должен быть `@Component`), или помечайте их тегом (`@Tag(Audit.class)`); иначе
  Kora 1.x валит сборку с «More than one component matches dependency type: OutboxHandler». Внедряйте
  `KafkaOutboxSender`, а не `KafkaOutboxHandler`.
- Класс с `@Component`, реализующий `OutboxHandler`, тоже заменяет обработчик по умолчанию.
- Декоратор вокруг `kafka.handler()` (логирование, метрики) должен делегировать не только `handle`, но и
  `handleBatch`. Иначе `handleBatch` по умолчанию отправляет по одной записи за round trip и превращает `NotAttempted`
  от cooldown в `Retry`, который тратит попытки.

### Потребление через inbox

Kafka доставляет at-least-once, как и outbox. `Inbox` записывает id сообщений в собственной транзакции потребителя,
поэтому потребитель обрабатывает каждое сообщение один раз:

```java
@Component
public final class OrdersListener {

    private final JdbcConnectionFactory db;
    private final Inbox inbox;

    public OrdersListener(JdbcConnectionFactory db, Inbox inbox) {
        this.db = db;
        this.inbox = inbox.scope("orders-projection"); // один scope на каждого независимого потребителя
    }

    @KafkaListener("kafka.consumer.orders")
    public void process(ConsumerRecords<String, byte[]> records) {
        db.inTx(() -> {
            var ids = new ArrayList<UUID>();
            for (var record : records) {
                var id = KafkaOutboxHeaders.messageId(record);
                if (id != null) {
                    ids.add(id);
                }
            }
            var fresh = new HashSet<>(inbox.tryAcceptAll(ids)); // один запрос на poll
            for (var record : records) {
                var id = KafkaOutboxHeaders.messageId(record);
                if (id == null || fresh.remove(id)) { // каждый впервые увиденный id — один раз
                    apply(record); // ваши записи, в той же транзакции
                }
            }
        });
    }
}
```

```kotlin
@KafkaListener("kafka.consumer.orders")
fun process(records: ConsumerRecords<String, ByteArray>) = db.inTx<Unit> { _: Connection ->
    val ids = records.map { KafkaOutboxHeaders.messageId(it) }
    val fresh = inbox.tryAcceptAll(ids.filterNotNull()).toHashSet()
    for ((i, record) in records.withIndex()) {
        val id = ids[i]
        if (id == null || fresh.remove(id)) apply(record)
    }
}
```

- `tryAccept(id)` возвращает true, когда scope видит id впервые (сообщение нужно обработать), и false для дубликата
  (его нужно пропустить). `tryAcceptAll(ids)` делает то же для целого poll одним запросом и возвращает id, увиденные
  впервые; id, повторённый во входном списке, возвращается не больше одного раза. Оба метода должны выполняться внутри
  `inTx`; откаченная транзакция ничего не записывает. Есть перегрузки с явным `Connection`.
- **Дедупликация работает в пределах scope потребителя.** Id, принятый в одном scope, остаётся новым для любого
  другого. Каждому независимому потребителю одних и тех же сообщений нужен свой scope: двум листенерам в разных
  consumer group на одном топике или двум сервисам с общей таблицей inbox, например `inbox.scope("orders-projection")`
  и `inbox.scope("mailer")`. Потребители с общим scope делят и его id: второй пропускает каждое сообщение, принятое
  первым. `Inbox` без scope — это scope `""`. Имя scope — от 1 до 64 символов из `A-Z a-z 0-9 _ . : -`; не меняйте его
  между деплоями, потому что под новым именем каждое сообщение окажется новым.
- **Принимайте каждый poll одним вызовом на транзакцию.** Id записываются в фиксированном порядке, поэтому
  параллельные потребители пересекающихся poll ждут друг друга, а не попадают в deadlock. Несколько вызовов accept в
  одной транзакции могут попасть в deadlock с параллельным потребителем (SQLState 40P01); тогда poll повторяется, как
  любая неудавшаяся транзакция.
- **Окно дедупликации.** Id дедуплицируется, пока его время UUIDv7 лежит в окне: id возрастом до
  `outbox.inbox.retention − outbox.inbox.partitionInterval` (по умолчанию 23 ч) дедуплицируются всегда. Id вне окна или
  не в формате UUIDv7 принимается без дедупликации (`true`, ничего не записывается, WARN не чаще раза в минуту и
  счётчик `outbox.inbox.outside_window`); транзакцию это никогда не валит. Задайте `retention` больше самого большого
  лага потребителя или повторного чтения (replay), которое нужно покрыть.

## Как работает relay

Каждый relay-воркер (`outbox-relay-N`, `relay.workers` на инстанс) делает тик, когда пора, читает курсоры всех шардов
и выполняет по шагу на каждом шарде, где есть работа. Работа есть, если в шард пришли строки после тика его курсора
(каждый тик записывает, в каких шардах они появились), у головы в retry-таблице или у отложенной записи наступил
срок, батч шарда начат и не закончен, или курсор шарда не двигался 5 с (тогда шаг просто сдвигает его вперёд для
обслуживания). Шаг шарда — одна транзакция (имена таблиц для базового имени `outbox`):

```mermaid
sequenceDiagram
    autonumber
    participant W as воркер outbox-relay-N
    participant DB as Postgres
    participant H as OutboxHandler
    W->>DB: тик, если пора: pg_current_snapshot() и шарды с новыми строками в outbox_tick
    Note over W,DB: дальше шаг одного шарда, одна транзакция
    W->>DB: pg_try_advisory_xact_lock(шард)
    break false: шард сейчас у другого воркера
        DB-->>W: шаг пропущен
    end
    W->>DB: последняя строка курсора шарда
    DB-->>W: тик P, last_id, цель C (начатого батча или новая)
    opt есть головы в outbox_retry или отложенные записи в outbox_parked, срок которых наступил
        W->>H: handleBatch(головы и следующие за ними строки, отложенные записи)
        H-->>W: outcomes
        W->>DB: удалить доставленные, перепланировать, перенести в outbox_dead
    end
    W->>DB: строки шарда из транзакций, закоммиченных между тиками P и C
    DB-->>W: чанк после last_id, не больше batchSize строк
    W->>H: handleBatch(отложенные записи ключей чанка,<br/>затем чанк без строк ключей, ждущих в outbox_retry)
    Note over W,H: 80 % batchTimeout: windDownRequested() и прерывание<br/>100 %: от вызова отказываются, все его записи Retry
    H-->>W: Delivered / Retry / Permanent / NotAttempted / Requeue
    W->>W: BatchClassifier: дальше или пауза
    alt дальше
        W->>DB: сбойные ключи в outbox_retry, Permanent в outbox_dead, Requeue в outbox_parked
    else пауза: ничего не доставлено
        Note over W,DB: ни одна строка не отводится, курсор встаёт на паузу с backoff
    end
    W->>DB: новая строка курсора: прогресс или пауза
    W->>DB: COMMIT снимает блокировку шарда
    Note over W,DB: следующий шаг этого шарда может сделать воркер любого инстанса
```

Почему батч между тиками P и C содержит ровно транзакции, закоммиченные между ними, — в
[Заметках о дизайне](#заметки-о-дизайне).

## Обработка сбоев

Путь записи после вызова обработчика (`headFailuresBeforeDivert` по умолчанию 3; `maxAttempts` по умолчанию 0, то
есть без предела попыток):

```mermaid
flowchart TD
    read["записи шага переданы обработчику"] --> pause{{"ничего не доставлено, а сбой на 2+ ключах<br/>или на одном, но неудачных попыток<br/>подряд меньше headFailuresBeforeDivert?"}}
    pause -->|"да: шард на паузе с backoff,<br/>те же записи читаются снова,<br/>чанком вдвое меньше"| read
    pause -->|нет| outcome{"outcome записи"}
    outcome -->|Delivered| ok(["доставлена"])
    outcome -->|Permanent| dead(["outbox_dead: ERROR в логе,<br/>ключ идёт дальше"])
    outcome -->|Requeue| parked(["outbox_parked: не сбой, ключ идёт дальше;<br/>запись вернётся перед следующей<br/>записью ключа или по сроку"])
    outcome -->|"Retry / NotAttempted"| head["ключ отведён в outbox_retry:<br/>запись — голова с next_attempt_at,<br/>следующие записи ключа — последователи"]
    head -.-> others(["остальные ключи идут дальше,<br/>новые записи этого ключа<br/>встают в последователи"])
    head --> due{{"срок головы наступил:<br/>голова и последователи<br/>снова у обработчика"}}
    due -->|"Retry / NotAttempted:<br/>новый backoff"| due
    due -->|"Permanent или<br/>исчерпан maxAttempts"| dead
    due -->|Requeue| parked
    due -->|Delivered| next{{"у ключа остались<br/>записи в outbox_retry?"}}
    next -->|"да: следующая<br/>становится головой"| due
    next -->|нет| free(["ключ разблокирован"])
```

Что relay делает с outcome — чтобы вы могли читать логи и метрики:

- **Часть записей доставлена, часть нет** (`Retry` или `NotAttempted`): пара (topic, key) каждой сбойной записи
  переходит в retry-таблицу `<base>_retry`. Сбойная запись становится головой ключа и повторяется с backoff
  (`initialDelay`, удваиваемый с каждой попыткой до `maxDelay`) или позже, если `notBefore` от обработчика позже
  (ограничено 24 ч); последующие записи этого ключа по порядку следуют за ней туда же.
  Остальные ключи идут дальше. WARN «streams diverted to retry», счётчик `outbox.messages.failed{outcome="retry"}`.
- **Ничего не доставлено, сбой на двух и более ключах:** цель, похоже, недоступна. Шард приостанавливается с тем же
  backoff, и ничего не записывается (WARN «paused», счётчик `outbox.shard.paused`); тики продолжают накапливаться, и
  шард продолжает с того места, где остановился.
- **Ничего не доставлено, сбоит один ключ** (ядовитое или медленное сообщение в голове либо шард с одним активным
  ключом): шард приостанавливается, а после `headFailuresBeforeDivert` (3) неудачных попыток подряд читает одну
  головную строку (режим пробы) и отводит этот ключ, так что остальной шард идёт дальше. Каждая неудачная попытка также
  уменьшает вдвое число строк, которые следующий шаг передаёт за вызов, а каждая доставка снова его удваивает, поэтому
  запись, проваливающая весь свой вызов глубоко внутри чанка, изолируется после нескольких коротких пауз. Этот
  адаптивный лимит хранится в памяти каждого инстанса; при нескольких инстансах шард переходит между ними, поэтому
  изоляция занимает до нескольких раз больше пауз.
- **`Permanent`:** запись переносится в `<base>_dead` (ERROR, `outbox.messages.failed{outcome="dead"}`), а её ключ
  идёт дальше.
- **`Requeue`** — не сбой: запись переносится в `<base>_parked`, ключ идёт дальше, паузы и backoff нет, попытка не
  тратится. Запись вернётся перед следующей записью своего ключа или по сроку ([Свой обработчик](#свой-обработчик)).
  Счётчик `outbox.messages.requeued`.
- **Головы в retry-таблице** повторяются на последующих шагах шарда вместе со следующими за ними записями. Ключ, у
  которого в retry-таблице не осталось строк, разблокируется. При `relay.retry.maxAttempts = N > 0` запись, провалившая
  N попыток, переносится в dead-таблицу. При значении по умолчанию 0 временные сбои никогда не приводят в dead letters.
- **Зависшая запись** (её вызов обработчика не завершается до `batchTimeout`) при `maxAttempts = 0` заставляет свой ключ
  повторяться вечно, пока другие ключи идут. Каждый раз, когда подходит её срок (не чаще раза в `maxDelay`), она
  обходится шагу своего шарда примерно в 2 × `batchTimeout`. Задайте `maxAttempts` или исправьте обработчик.

## Справочник по конфигурации

Все ключи необязательны. Длительности в HOCON: `50ms`, `30s`, `5m`, `1h`. Значения проверяются при старте; некорректное
значение валит старт до выполнения любого DDL.

### `outbox`

| Ключ | По умолчанию | Значение |
|---|---|---|
| `table` | `"outbox"` | Базовое имя всех таблиц outbox. `[schema.]name`, каждая часть `[A-Za-z_][A-Za-z0-9_]{0,40}`, приводится к нижнему регистру, как Postgres приводит имена без кавычек. Имя без схемы означает таблицу в `current_schema()`. |
| `shards` | `32` | 1..32767. Единицы параллельной работы relay: каждый шард одновременно обрабатывается не больше чем одним воркером по всем инстансам, и каждый ключ остаётся в одном шарде. Шард без новых строк не обрабатывается, кроме одного шага раз в 5 с, который сдвигает его курсор. Изменение требует пустого outbox ([Решардинг](#решардинг)). |

### `outbox.relay`

| Ключ | По умолчанию | Значение |
|---|---|---|
| `enabled` | `true` | `false`: этот инстанс не запускает relay-воркеров **и обслуживание партиций**; `OutboxRelay.drain()` по-прежнему работает. Держите хотя бы один инстанс с включённым relay ([Обслуживание работает там же, где relay](#обслуживание-работает-там-же-где-relay)). |
| `workers` | `4` | Число потоков relay на инстанс (`outbox-relay-N`), ≥ 1. Каждый держит соединение с БД, пока работает. |
| `batchSize` | `1000` | ≥ 1. Максимум строк на одно чтение чанка и на один вызов обработчика. Вызов с чанком несёт сверх него до текущего лимита чанка (не больше `batchSize`) отложенных записей ключей чанка, то есть всего до `2 × batchSize` записей. Новый батч охватывает около `batchSize × shards` id, а проход по retry-таблице передаёт не больше `batchSize` строк за шаг (головы со следующими за ними строками и отложенные записи, срок которых наступил). |
| `maxTicksPerBatch` | `100` | ≥ 1. Наибольшее число тиков, которое может охватить один батч. |
| `minTickInterval` | `50ms` | ≥ 0. Под нагрузкой тики идут не чаще, чем раз в этот интервал; коммит ждёт своего тика примерно до удвоенного интервала. |
| `idlePollInterval` | `200ms` | > 0. Частота idle-тиков (примерно один тик за интервал на весь кластер) и то, как быстро замечается коммит на инстансе без локального пробуждения. |
| `batchTimeout` | `30s` | > 0. Предел одного вызова обработчика: сворачивание на 80 %, отказ от вызова на 100 % с повтором всех его записей ([Дедлайны](#свой-обработчик)). `drain()` сдаётся после 2 × `batchTimeout` без прогресса. С обработчиком Kafka должен быть не меньше `delivery.timeout.ms + 5 s + max.block.ms`. |
| `shutdownTimeout` | `30s` | ≥ 0. При остановке незавершённым батчам даётся столько времени, затем их воркеры прерываются, а батчи позже доставляются повторно. Столько же получает выполняющийся запуск обслуживания inbox. |
| `headFailuresBeforeDivert` | `3` | ≥ 1. Число неудачных попыток подряд, после которого шард читает одну головную строку и отводит единственный сбойный ключ. |

### `outbox.relay.retry`

| Ключ | По умолчанию | Значение |
|---|---|---|
| `initialDelay` | `1s` | > 0. Первый backoff для головы в retry-таблице и для паузы шарда; удваивается с каждым сбоем. |
| `maxDelay` | `5m` | ≥ `initialDelay`. Потолок backoff. |
| `maxAttempts` | `0` | ≥ 0. Запись, провалившая столько попыток, переносится в dead-таблицу; `0` — повторять вечно. `NotAttempted` не считается. |

### `outbox.relay.requeue`

Записи, отложенные обработчиком через `Outcome.requeue()` ([Свой обработчик](#свой-обработчик)).

| Ключ | По умолчанию | Значение |
|---|---|---|
| `delay` | `1s` | > 0. Через сколько отложенная запись возвращается к обработчику, если раньше не придёт новая запись её ключа; с `requeue(notBefore)` — не раньше `notBefore` (не дальше 24 ч). Отложенная запись, которую обработчик не попробовал, тоже ждёт не меньше `delay`. |
| `maxRequeues` | `0` | ≥ 0. Сколько раз запись можно отложить: `requeue` сверх этого переносит её в dead-таблицу с ошибкой «requeue limit reached», `attempts` не меняется. Считается каждый ответ `requeue`, в том числе на запись, подмешанную в вызов перед новой записью ключа. `0` — без предела. |

### `outbox.partitions`

| Ключ | По умолчанию | Значение |
|---|---|---|
| `xidsPerPartition` | `16777216` | ≥ 1000. Размер одной партиции-диапазона по xid (2^24 xid, около 5.6 мин при 50 000 transaction id в секунду на весь кластер). |
| `premake` | `64` | ≥ 2. Сколько диапазонов поддерживается выше текущего xid. Обслуживание расширяет их, когда израсходована половина. Их должно хватить на вашу самую длинную транзакцию или снапшот ([Партиции](#партиции)). |
| `maintenanceInterval` | `30s` | > 0. Периодичность обслуживания outbox и обслуживания inbox. Также то, как часто каждый шард заново полностью перепроверяет retry-таблицу и таблицу отложенных записей. |

### `outbox.inbox` (`InboxModule`)

| Ключ | По умолчанию | Значение |
|---|---|---|
| `table` | `"inbox"` | Имя таблицы inbox, правила те же, что для `outbox.table`. Создаётся, только когда какой-то компонент использует `Inbox`. |
| `partitionInterval` | `1h` | ≥ 1m. Длина одного диапазона inbox по времени UUIDv7. |
| `premake` | `24h` | ≥ 2 × `partitionInterval`, и `premake − partitionInterval` должно превышать `outbox.partitions.maintenanceInterval`. Диапазоны поддерживаются до `now + premake`. |
| `retention` | `24h` | ≥ 2 × `partitionInterval`. Диапазоны удаляются, когда становятся старше этого значения. Id возрастом до `retention − partitionInterval` дедуплицируются всегда. `(retention + premake) / partitionInterval` ≤ 1000. |

Inbox также читает `outbox.partitions.maintenanceInterval` (расписание своего обслуживания) и
`outbox.relay.shutdownTimeout` (сколько времени при остановке даётся выполняющемуся запуску обслуживания inbox).
Сервис только с inbox (один `InboxModule`) задаёт их в тех же местах.

### `outbox.kafka` (`KafkaOutboxModule`)

| Ключ | По умолчанию | Значение |
|---|---|---|
| `driverProperties` | — | Свойства продюсера Kafka. `bootstrap.servers` обязателен. Навязываемые, отвергаемые и задаваемые по умолчанию настройки: [Требования к Kafka](#требования-к-kafka). |

### Обслуживание работает там же, где relay

Обслуживание партиций (расширение, удаление пройденных relay партиций, очистка тиков и курсоров, gauges) планирует
relay: на инстансе с `relay.enabled = false` оно не выполняется. В деплое, где relay выключен на всех инстансах, запись
продолжает работать, но заранее созданные партиции заканчиваются (строки тогда идут в хвост: «tail overflow»), тики
копятся, а gauges остаются NaN. Держите хотя бы один инстанс с включённым relay. Деплой, который только вызывает
drain, может сам вызывать `Maintenance.runOnce()` каждые `maintenanceInterval` (компонент `Maintenance` есть в графе;
он лежит в пакете `internal`, поэтому стабильным API не является). Обслуживание inbox от этого не зависит: оно
работает на каждом инстансе, который использует `Inbox`.

## Требования к Kafka

**Топики.** Продюсер идемпотентный, с `acks=all`, но `acks=all` надёжен ровно настолько, насколько надёжен топик:

- replication factor ≥ 3, `min.insync.replicas` ≥ 2 и `unclean.leader.election.enable=false`. При
  `min.insync.replicas=1` (значение брокера по умолчанию) подтверждённая запись может потеряться при отказе её лидера,
  и тогда идемпотентный продюсер пишет последующие записи ключа дальше, через образовавшийся пропуск;
- добавление партиций в топик переносит ключи в другие партиции, поэтому потребители видят записи ключа до и после
  изменения в разных партициях, не по порядку. Создавайте топики сразу с итоговым числом партиций;
- создавайте топики и выдавайте ACL **до** деплоя писателей. Отсутствующий, недоступный по правам или некорректный
  топик не стопорит другие топики: его записи повторяются, а топик уходит на cooldown (1 с, удваиваясь до 30 с, одна
  проба по истечении каждого), но каждая проба отсутствующего топика блокирует воркер на время до `max.block.ms`. С
  включённым на брокере auto-create опечатка молча создаёт топик. Некорректное имя топика (`InvalidTopicException`) —
  постоянный сбой.

**Настройки продюсера** (`outbox.kafka.driverProperties`), проверяются при старте:

| | Настройки |
|---|---|
| Навязываются | `enable.idempotence=true`, `acks=all`, сериализаторы ключа и значения в byte array |
| Отвергаются | `transactional.id`; `partitioner.class` (порядку и cooldown нужно хеширование ключей Kafka); `partitioner.ignore.keys=true`; `acks`, отличный от `all`/`-1`; `enable.idempotence=false`; `max.in.flight.requests.per.connection` > 5; `delivery.timeout.ms` < `linger.ms + request.timeout.ms`; всё, что отвергает собственный `ProducerConfig` Kafka (например, `retries=0` вместе с идемпотентностью) |
| По умолчанию, если не заданы | `delivery.timeout.ms=20000`; затем, если `delivery.timeout.ms` тоже не был задан, `request.timeout.ms=15000`; `max.block.ms=5000`; `enable.metrics.push=false` |

- Не добавляйте интерцепторы продюсера (`interceptor.classes`), которые меняют ключ или партицию записи: от них
  зависит порядок по ключу.
- **Бюджет относительно `batchTimeout`.** Один вызов обработчика ждёт ack до `delivery.timeout.ms + 5 s` от своего
  начала, и сверху может добавиться не больше одного ожидания метаданных (`max.block.ms`): 20 + 5 + 5 = 30 с при
  значениях по умолчанию, ровно `outbox.relay.batchTimeout` по умолчанию. Держите
  `batchTimeout ≥ delivery.timeout.ms + 5 s + max.block.ms`; иначе модуль пишет WARN при старте. Relay сворачивает
  вызов на 80 % от `batchTimeout`: отправитель тогда сразу останавливается и возвращает подтверждённые записи как
  доставленные, неподтверждённые — как `Retry` (возможны дубликаты), а остальные — как `NotAttempted`. Так медленный
  кластер превращается в повторы, а не в застрявшие шарды.
- **Outcome.** `RecordTooLargeException`, `InvalidRecordException` и `InvalidTopicException` — постоянные сбои
  (dead-таблица); ключ null в compacted-топике даёт `InvalidRecordException`. Всё остальное, включая ошибки
  авторизации, повторяется.
- **Частичные сбои.** Партиция (или топик), отправки в которую сбоят сами по себе, уходит на cooldown на 1 с,
  удваиваясь до 30 с, а успешная отправка его сбрасывает. Записи с ключом для неё получают `NotAttempted` с этим
  `notBefore`, поэтому их ключи ждут в retry-таблице, пока остальной топик идёт. Когда cooldown заканчивается, один
  вызов её пробует. После таймаута метаданных остаток этого вызова пропускает топики, метаданные которых его продюсер
  ещё не получил: не больше одного ожидания метаданных на вызов.
- **Метрики.** Продюсеры библиотеки не привязаны к Micrometer: клиентские метрики Kafka видны только через JMX.
- Пул держит примерно по одному продюсеру на relay-воркер; каждый вызов берёт один из них.

## Эксплуатация

### Таблицы

Bootstrap создаёт эти таблицы, если их нет (показано для базового имени по умолчанию `outbox` и inbox с именем
`inbox`). Просмотрите их перед первым деплоем.

```sql
-- горячая таблица: только вставки, партиционирование по диапазонам xid пишущей транзакции
create table outbox (
  id          bigint generated always as identity,  -- последовательность outbox_id_seq
  shard       smallint    not null,
  xid         xid8        not null default pg_current_xact_id(),
  created_at  timestamptz not null default clock_timestamp(),
  message_id  uuid        not null,
  topic       text        not null,
  key         bytea,
  payload     bytea,
  headers     bytea
) partition by range (xid);
create index outbox_shard_xid on outbox (shard, xid);
-- диапазоны: outbox_x<lower>  for values from ('<lower>') to ('<lower + xidsPerPartition>')
-- хвост:     outbox_tail      for values from ('<верх самого высокого диапазона>') to (maxvalue)

create table outbox_tick (     -- журнал снапшотов
  id         bigint generated always as identity primary key,
  snapshot   pg_snapshot not null,
  created_at timestamptz not null default clock_timestamp(),
  max_id     bigint,           -- верхняя отметка outbox_id_seq на момент тика
  shards     smallint[]        -- шарды, получившие строки с предыдущего тика; null = все
);
create index outbox_tick_xmax on outbox_tick ((pg_snapshot_xmax(snapshot)));

create table outbox_cursor (   -- прогресс по шардам, журнал только со вставками
  shard          smallint not null,
  seq            bigint generated always as identity,
  tick_id        bigint not null,
  target_tick_id bigint,
  last_id        bigint not null default 0,
  paused_until   timestamptz,
  failures       int not null default 0,
  retry_due_at   timestamptz,
  parked_due_at  timestamptz,
  primary key (shard, seq)
);

create table outbox_retry (    -- заблокированные потоки (topic, key): голова (с заданным next_attempt_at) и следующие за ней строки
  id bigint primary key, shard smallint not null, message_id uuid not null, topic text not null,
  key bytea, payload bytea, headers bytea, created_at timestamptz not null,
  attempts int not null default 0, next_attempt_at timestamptz, last_error text, requeues int not null default 0
);
create index outbox_retry_key on outbox_retry (shard, topic, key, id);
create index outbox_retry_due on outbox_retry (shard, next_attempt_at) where next_attempt_at is not null;

create table outbox_parked (   -- отложенные через Outcome.requeue: ключ не блокируют, ждут новой записи ключа или срока
  id bigint primary key, shard smallint not null, message_id uuid not null, topic text not null,
  key bytea, payload bytea, headers bytea, created_at timestamptz not null,
  attempts int not null default 0, requeues int not null default 0, due_at timestamptz not null
);
create index outbox_parked_key on outbox_parked (shard, topic, key, id);
create index outbox_parked_due on outbox_parked (shard, due_at);

create table outbox_dead (     -- dead letters, хранятся, пока вы их не удалите
  id bigint primary key, shard smallint not null, message_id uuid not null, topic text not null,
  key bytea, payload bytea, headers bytea, created_at timestamptz not null,
  attempts int not null, failed_at timestamptz not null default now(), error text
);

create table outbox_halt (     -- одна строка, пока relay остановлен после регрессии xid
  id int primary key default 1 check (id = 1),
  halted_at timestamptz not null default now(),
  max_tick_xmax xid8 not null
);

create table outbox_maintenance (  -- одна строка: когда последний запуск обслуживания завершился на любом инстансе
  id int primary key default 1 check (id = 1),
  succeeded_at timestamptz not null
);

create table outbox_dropping (     -- диапазоны, которые удаляет обслуживание: с момента перед detach до drop
  table_name text primary key,
  at timestamptz not null default now()
);

-- inbox (InboxModule), создаётся, когда какой-то компонент использует Inbox
create table inbox (
  message_id  uuid        not null,
  consumer    text        not null default '',  -- scope
  received_at timestamptz not null default now(),
  primary key (message_id, consumer)
) partition by range (message_id);
-- диапазоны: inbox_t<yyyyMMddHH> (inbox_t<yyyyMMddHHmm>, если partitionInterval не кратен часу), по времени UUIDv7, UTC
```

- Таблицы вида `<base>_x<цифры>` в схеме outbox и `<inbox>_t<10 или 12 цифр>` в схеме inbox принадлежат библиотеке:
  пустую такую таблицу она может удалить, а на непустую пишет ERROR о сироте. Не создавайте таблицы с такими именами.
- Identity-последовательности `<base>.id`, `<base>_tick.id` и `<base>_cursor.seq` должны оставаться с `CACHE 1`:
  кешированные id назначаются не в порядке блокировок, и это ломает порядок по ключу. Старт падает, если у одной из
  них кеш больше.
- Bootstrap выполняет каждый запрос с `statement_timeout` 1 мин и ждёт любую блокировку не больше 5 с (блокировку
  схемы — до минуты, пока идёт bootstrap других инстансов). Запрос, которому нужно больше, например построение
  недостающего индекса на заполненной таблице, валит старт с SQLState 57014: создайте индекс вручную по определению
  выше, затем перезапустите сервис. На существующих таблицах bootstrap не берёт блокировок, за которыми ждали бы
  писатели.
- DDL библиотеки никогда не берёт `ACCESS EXCLUSIVE` на родительскую таблицу outbox.
- Ваша DDL с `ACCESS EXCLUSIVE` на любую партицию outbox (`ALTER TABLE`, `VACUUM FULL`, `CLUSTER`, `TRUNCATE`,
  `REINDEX` без `CONCURRENTLY`, pg_repack) останавливает доставку по всем шардам, пока держит или ждёт блокировку:
  запрос батча блокирует все партиции. Тики при этом идут, растёт `outbox.shard.lag`. Берите варианты с
  `CONCURRENTLY` и запускайте такое в спокойное время.
- Не удаляйте и не очищайте (truncate) таблицы библиотеки, пока работают инстансы. Например, без `<base>_maintenance`
  каждый запуск обслуживания падает, пока рестарт заново не создаст таблицу.

### Партиции

Таблица outbox партиционирована по xid пишущей транзакции, диапазонами по `xidsPerPartition`:

- **Создаются заранее.** Обслуживание держит `premake` диапазонов выше текущего xid и расширяет их, когда израсходована
  половина: конкурентно отсоединяет пустой хвост, удаляет его, создаёт новые диапазоны и присоединяет новый хвост. На
  новой таблице их создаёт bootstrap.
- **Удаляются после relay.** Диапазон удаляется (`DETACH CONCURRENTLY`, затем `DROP TABLE`), когда его прошли все
  шарды: когда его верхняя граница не выше snapshot xmin тика самого медленного шарда. Никакого `DELETE`, никакого
  vacuum горячей таблицы и никакой зависимости от часов.
- **Хвост** `[верх самого высокого диапазона, MAXVALUE)` принимает строки, если обслуживание отстаёт. Строки в нём
  проходят relay как обычно, но его место не освобождается, пока не вмешается оператор («tail overflow»).
- DDL партиций выполняется с `lock_timeout` (2 с для create и attach, 30 с для detach) и собственным
  `statement_timeout`; после таймаута операция повторяется на следующем запуске.

Жизнь диапазонов при базовом имени `outbox` и настройках по умолчанию: диапазоны `outbox_x<lower>` создаются заранее
впереди текущего xid, хвост `outbox_tail` принимает перелив, а пройденные диапазоны удаляются целиком, без `DELETE`.

```mermaid
flowchart TD
    subgraph axis["outbox: xid растёт сверху вниз"]
        old["пройденные<br/>outbox_x…"] --> hot["outbox_x…<br/>с текущим xid:<br/>сюда идут вставки"]
        hot --> ahead["premake = 64<br/>диапазона впереди,<br/>по 16 777 216 xid"]
        ahead --> tail["outbox_tail<br/>до MAXVALUE:<br/>перелив"]
    end
    ahead -.->|"впереди ≤ premake / 2"| ext["расширение: detach и drop<br/>пустого хвоста, новые<br/>диапазоны, новый хвост"]
    old -->|"верх ≤ xmin тика<br/>самого медленного шарда"| mark["отметка в outbox_dropping"]
    mark --> detach["DETACH PARTITION … CONCURRENTLY"]
    detach --> drop["DROP TABLE<br/>и снять отметку, если есть"]
    detach -.->|"инстанс упал до DROP"| orphan{{"отсоединённый outbox_x…:<br/>с отметкой — нет строк от горизонта шардов,<br/>без отметки — пуст?"}}
    orphan -->|да| drop
    orphan -->|нет| keep(["остаётся: ERROR и<br/>outbox.partitions.orphaned"])
```

**Длинные снапшоты и `premake`.** Хвост отсоединяется, только когда ничто не может оставить этот detach
незавершённым. Обслуживание откладывает расширение с WARN не чаще раза в минуту, пока любая сессия базы держит
снапшот в транзакции, открытой дольше 20 с (отчёт в repeatable read, долгий пакетный job; pg_dump и начальная
синхронизация логической репликации — любой длительности), либо держит блокировку на outbox в такой транзакции, либо
ждёт её. Тем временем заранее созданные диапазоны расходуются. Если они кончатся раньше, строки пойдут в
присоединённый хвост (безопасная деградация, «tail overflow»), но не в ошибки. Поэтому выбирайте `premake` так, чтобы
его хватило на самый длинный снапшот:

```
premake / 2 × xidsPerPartition  ≥  пиковое число transaction id в секунду (весь кластер) × самый длинный снапшот в секундах
```

Пример: 4-часовой pg_dump, пока кластер расходует 20 000 xid/с, занимает 288 M xid — 17.2 диапазона размера по
умолчанию, поэтому `premake / 2` ≥ 18: `premake` ≥ 36. Значение по умолчанию 64 оставляет запас в 32 диапазона
(537 M xid).

Остаточный случай: читатель, которому на момент проверки обслуживания меньше 20 с и который затем держит блокировку
на outbox дольше 30 с, может оставить detach хвоста незавершённым. Пока detach не завершён, ни одна партиция не
принимает строки выше самого высокого диапазона, и вставки начинают падать, когда текущий xid до него доходит. Gauge
`outbox.partitions.xids_left` тогда показывает, сколько xid осталось (NaN, пока хвост присоединён), и на каждом
запуске пишется ERROR; обслуживание завершает detach, как только читатель заканчивает.

Длинные транзакции не стопорят relay (см. [Заметки о дизайне](#заметки-о-дизайне)), но до своего окончания не дают
удалять пройденные партиции и замедляют все таблицы с интенсивными update, включая retry-таблицу.

### Алерты

| Сигнал | Значение |
|---|---|
| `outbox.maintenance.since_success` > 5 × `maintenanceInterval` | Ни один запуск обслуживания не завершился ни на одном инстансе: партиции не расширяются и не удаляются. В логах также WARN. |
| `outbox.partitions.premade` < 2 или `outbox.partitions.overflow` = 1 | Обслуживание отстало; строки идут или скоро пойдут в хвост («tail overflow»). |
| `outbox.partitions.xids_left` не NaN | Хвост не присоединён; вставки начнут падать, когда значение дойдёт до 0. |
| `outbox.partitions.orphaned` > 0, `outbox.inbox.partitions.orphaned` > 0 | Отсоединённый диапазон содержит строки ([Осиротевшие партиции](#осиротевшие-партиции)). |
| `outbox.maintenance.transaction_pooling` = 1 | Обслуживание пропускается: обнаружен transaction pooler. |
| `outbox.shard.lag` растёт | Самый медленный шард отстаёт (цель недоступна, медленный обработчик, мало воркеров). |
| `outbox.dead.size` > 0 | Есть dead letters, которые нужно разобрать. |
| ERROR «outbox relay halted» | Регрессия xid («xid regression recovery»). |
| WARN «outbox tick made without its shard set» | Проба шардов при записи тика не прошла, чаще всего потому, что партицию держит ACCESS EXCLUSIVE (ALTER TABLE, VACUUM FULL, TRUNCATE, LOCK вручную). Тики идут дальше без набора шардов, и relay шагает по всем шардам; пока партиция заблокирована, шаги ждут её, и растёт `outbox.shard.lag`. Не чаще раза в минуту. |

### Dead letters

Запись переносится в `<base>_dead`, когда сбоит постоянно (`OutboxPermanentException`, `Outcome.permanent`,
`RecordTooLargeException` из Kafka и подобные), исчерпывает `maxAttempts` или откладывается сверх
`relay.requeue.maxRequeues` (ошибка «requeue limit reached»). Каждый перенос пишет ERROR с id сообщения. Мёртвые
строки хранятся, пока вы их не удалите; их считает `outbox.dead.size`.

Просмотр:

```sql
select id, message_id, topic, encode(key, 'escape') as key, attempts, failed_at, error
  from outbox_dead
 order by id;
```

**Переотправляйте (redrive) через таблицу outbox, никогда — через retry-таблицу.**

Процедура ещё не отрепетирована авторами на живой базе — прогоните её на копии (staging) перед использованием в продакшене.

Перенесите строки обратно в `<base>` одной транзакцией. Каждая становится новым сообщением с новыми id и xid, но
сохраняет свой `message_id`. Поэтому, если сообщение всё-таки дошло, потребители дедуплицируют его, пока его id
находится в их окне inbox (`retention − partitionInterval`, по умолчанию 23 ч):

```sql
with moved as (
  delete from outbox_dead
   where id = any('{1001,1002}')  -- строки для переотправки
  returning id, shard, message_id, topic, key, payload, headers
)
insert into outbox (shard, message_id, topic, key, payload, headers)
select shard, message_id, topic, key, payload, headers from moved order by id;
```

- Переотправленная строка доставляется после тех сообщений её ключа, которые были отправлены до redrive. Этот SQL не
  берёт блокировку ключа, поэтому `send` того же ключа, коммитящийся параллельно, может оказаться как до, так и после:
  если это важно, переотправляйте, когда по ключу нет трафика.
- Сохранённый `shard` корректен, только пока `outbox.shards` не менялся. После решардинга переотправляйте через
  `Outbox.send` (тогда сообщение получит новый id).
- Не вставляйте строки в `<base>_retry` и `<base>_parked`, не удаляйте их оттуда и не меняйте их порядок вручную.
  Relay полностью перечитывает эти таблицы только раз в `maintenanceInterval` на шард; до этого он может доставить
  более поздние строки такого ключа первыми или создать для него вторую голову.

### Переполнение хвоста (tail overflow)

Процедура ещё не отрепетирована авторами на живой базе — прогоните её на копии (staging) перед использованием в продакшене.

**Симптомы:** ERROR «outbox partition horizon reached» или «partition tail ... holds rows», gauge
`outbox.partitions.overflow` = 1, низкий `outbox.partitions.premade`. Обслуживание не смогло вовремя расширить
партиции (оно не работало, transaction pooling, длинные снапшоты, таймауты блокировок), и текущий xid дошёл до хвоста,
либо в хвосте есть строки. Строки в хвосте по-прежнему проходят relay, поэтому ничего не теряется, но хвост продолжает
расти и никогда не может быть удалён: библиотека никогда не отсоединяет хвост, который используется. Сначала устраните
причину (см. `outbox.maintenance.since_success` и строки WARN/ERROR), затем превратите хвост в обычный диапазон. Для
базового имени `outbox` и `xidsPerPartition` S:

1. Прочитайте нижнюю границу хвоста H и текущий xid X:

   ```sql
   select pg_get_expr(relpartbound, oid) from pg_class where oid = 'outbox_tail'::regclass; -- FROM ('H') TO (MAXVALUE)
   select pg_snapshot_xmax(pg_current_snapshot());                                       -- X
   ```

2. Выберите U = (floor(X / S) + k) × S: k диапазонов выше текущего, причём k × S должно с запасом превышать число xid,
   которое кластер израсходует за время этой процедуры плюс один `maintenanceInterval`, а k — быть заметно меньше
   `premake` (k = 4 — хорошее начало).

3. Добавьте check-ограничение, на которое может опереться attach, чтобы не сканировать хвост под эксклюзивной
   блокировкой. `NOT VALID` плюс `VALIDATE` позволяет писателям продолжать работу во время сканирования. Ограничение
   сразу действует на новые строки, поэтому U должно оставаться впереди текущего xid, пока не выполнен шаг 4:

   ```sql
   set lock_timeout = '2s';  -- ADD CONSTRAINT блокирует хвост, куда идёт каждая новая строка: писатели ждут за ним не дольше этого
   alter table outbox_tail add constraint outbox_tail_bounded check (xid >= 'H' and xid < 'U') not valid;
   reset lock_timeout;
   alter table outbox_tail validate constraint outbox_tail_bounded;  -- SHARE UPDATE EXCLUSIVE: писатели продолжают работу
   ```

   Таймаут блокировки (SQLState 55P03) ничего не меняет: повторите. Пока хвост держит pg_dump, другая длинная
   транзакция, которая трогала outbox, или шаг relay, ждущий медленного вызова обработчика, команда будет падать;
   повторите, когда они закончатся. Пропускает ли ATTACH сканирование при таком check-ограничении по xid8, не
   проверялось.

4. Замените хвост диапазоном `[H, U)` в одной короткой транзакции. Получив блокировку, она держит `ACCESS EXCLUSIVE` на
   `outbox` несколько миллисекунд; пока она ждёт (не дольше `lock_timeout`), новые писатели встают в очередь за ней,
   поэтому делайте её короткой и запускайте в спокойное время. Если любой запрос упадёт (таймаут блокировки, идущий DDL
   партиций от обслуживания), транзакция откатится без изменений: повторите её. Поскольку это одна транзакция,
   обслуживание никогда не увидит отсоединённую таблицу в промежуточном состоянии.

   ```sql
   begin;
   set local lock_timeout = '2s';
   set local statement_timeout = '10s';  -- если ATTACH всё же станет сканировать хвост: откат, а не удержание всех писателей
   alter table outbox detach partition outbox_tail;
   alter table outbox_tail rename to outbox_xH;  -- именование библиотеки: outbox_x<нижняя граница>
   alter table outbox attach partition outbox_xH for values from ('H') to ('U');
   commit;
   ```

5. Следующий запуск обслуживания не находит хвоста, создаёт диапазоны от U до `premake` вперёд и новый хвост и
   сбрасывает `outbox.partitions.overflow`. До этого вставки с xid не ниже U падали бы, но выбор k это исключает.
   Бывший хвост удаляется, как любой диапазон, когда его пройдёт relay. Check-ограничение можно оставить.

### Осиротевшие партиции

Диапазон, оставленный отсоединённым, но не удалённым, лежит вне outbox: relay не видит его строк. Обслуживание
разбирается с такими диапазонами на каждом запуске:

- диапазон, который библиотека сама отсоединила как пройденный (падение между detach и drop): удаляется, когда в нём не
  остаётся строк на горизонте самого медленного шарда или выше;
- диапазон, отсоединённый кем-то другим: удаляется, только если он пуст;
- всё остальное сохраняется с ERROR, где названы таблица и запрос для восстановления, и учитывается в
  `outbox.partitions.orphaned`.

**Никогда не отсоединяйте партиции outbox вручную.** Диапазон, отсоединённый до того, как до него дошёл relay, прячет
от relay свои строки. Чтобы восстановить сохранённую сироту, перенесите её недоставленные строки обратно и удалите её.

Процедура ещё не отрепетирована авторами на живой базе — прогоните её на копии (staging) перед использованием в продакшене.

```sql
begin;
insert into outbox (shard, message_id, topic, key, payload, headers)
select shard, message_id, topic, key, payload, headers
  from outbox_x83886080
 where xid >= '<горизонт из ERROR>'::xid8  -- строки ниже горизонта relay уже прошёл
 order by id;
drop table outbox_x83886080;
commit;
```

Для диапазона, отсоединённого кем-то другим, горизонт из ERROR не применим: сравните его строки с тем, что получили
ваши потребители, и перенесите обратно недоставленные. Перенесённые строки — новые сообщения (новые id и xid, те же
message id), они доставляются после более поздних сообщений своих ключей. Скопированный `shard` корректен, только пока
`outbox.shards` не менялся.

**Диапазоны inbox.** Истёкший диапазон inbox, оставленный отсоединённым, удаляется. Диапазон, отсоединённый внутри
окна дедупликации, сохраняется, и о нём сообщается (`outbox.inbox.partitions.orphaned`, ERROR): id из его временного
диапазона не дедуплицируются, а если у inbox есть диапазоны по обе стороны от него, вставка такого id падает (SQLState
23514), и это валит транзакцию потребителя. Присоедините его обратно запросом `alter table ... attach partition ...`,
который приводится в ERROR.

### Восстановление после регрессии xid (xid regression recovery)

Relay находит новые сообщения, сравнивая идентификаторы транзакций со снапшотами в `<base>_tick`. Если счётчик xid
базы опускается ниже xmax сохранённого тика, новые строки выглядят уже пройденными и пропускаются. Это случается после
**логического** восстановления или миграции: pg_dump/pg_restore в другой кластер или логическая репликация в новый
кластер. Физический бэкап, PITR или failover на streaming-реплику переносят счётчик вместе с данными, поэтому к этому
не приводят (к чему они приводят, см. в разделе [Репликация, failover и PITR](#репликация-failover-и-pitr)).

**Что вы увидите.**

- Инстанс, стартующий на такой базе, падает при старте («xid counter went backwards ... see README "xid regression
  recovery"»), либо, если воркеры с тех пор уже сделали тики, стартует и останавливается на первой проверке.
- Обслуживание работающего инстанса записывает остановку в `<base>_halt` (одна строка с наибольшим xmax тика) и пишет
  ERROR не чаще раза в минуту. Ни один relay-воркер ни на одном инстансе не берёт шарды, а `drain()` бросает
  исключение. Остановка переживает рестарты.
- Запись в outbox падает с SQLState 23514, пока счётчик ниже самого нижнего диапазона xid. Выше него запись проходит,
  но с xid, которые старые тики считают уже пройденными: relay никогда их не доставит, и спасает их только условие по
  `created_at` из шага 3. Остановите писателей, как только увидите остановку relay.

Маркер удаляется автоматически, только когда счётчик xid не ниже xmax каждого тика **и** наибольший xmax тика ниже
значения, записанного в маркере. Того, что счётчик догнал прежние значения, недостаточно: курсоры всё ещё стояли бы на
тиках до регрессии и пропустили бы то, что было записано за это время. Никогда не удаляйте эту строку, не починив тики
и курсоры, как это делает шаг 5.

**Процедура** (базовое имя `outbox`; подставьте свои имена):

Процедура ещё не отрепетирована авторами на живой базе — прогоните её на копии (staging) перед использованием в продакшене.
Там же сравните `outbox_unrelayed` с тем, что получили потребители.

1. Остановите все инстансы.
2. Найдите отдельно стоящие таблицы `outbox_x%`: новый диапазон с тем же именем подхватил бы такую таблицу вместе со
   строками.

   ```sql
   select relname from pg_class c where relname ~ '^outbox_x[0-9]+$' and relkind = 'r'
      and not exists (select 1 from pg_inherits where inhrelid = c.oid);
   ```

   Удалите пустые. Каждую, где есть строки, переименуйте так, чтобы имя выпало из шаблона библиотеки
   (`alter table outbox_x83886080 rename to outbox_orphan_83886080`) и ни один новый диапазон её не подхватил. После
   шага 7 обращайтесь с ней как с диапазоном, отсоединённым кем-то другим: сравните её строки с тем, что получили
   потребители, перенесите обратно недоставленные, затем удалите её.

3. Сохраните то, что не прошло relay: по каждому шарду — строки, которых не видит тик его курсора, плюс все строки,
   записанные в восстановленную базу (T — момент, когда был снят снапшот бэкапа, или момент логического переключения;
   более раннее T лишь добавляет дубликаты). Записывайте T со смещением относительно UTC, например
   `timestamptz '2026-10-26 09:00+00'`: без смещения Postgres читает значение в TimeZone сессии, и если T окажется
   позже реального момента, строки, записанные в промежутке, потеряются. Строки, которые уже лежат в retry-таблице
   или среди отложенных (`outbox_parked`), остаются там, а мёртвые строки повторно не отправляются.

   ```sql
   create table outbox_unrelayed as
   select o.id, o.shard, o.created_at, o.message_id, o.topic, o.key, o.payload, o.headers
     from outbox o
     cross join lateral (select c.tick_id from outbox_cursor c
                          where c.shard = o.shard order by c.seq desc limit 1) c
     join outbox_tick t on t.id = c.tick_id
    where (not pg_visible_in_snapshot(o.xid, t.snapshot)
           or o.created_at >= timestamptz '<T>' - interval '10 minutes')  -- записаны после бэкапа или переключения
      and not exists (select 1 from outbox_retry r where r.id = o.id and r.message_id = o.message_id)
      and not exists (select 1 from outbox_parked p where p.id = o.id and p.message_id = o.message_id)
      and not exists (select 1 from outbox_dead d where d.id = o.id and d.message_id = o.message_id);
   ```

4. Сделайте так, чтобы новые id были выше всех старых. pg_restore восстанавливает последовательность, логическая
   репликация — нет:

   ```sql
   select setval(pg_get_serial_sequence('outbox', 'id'), greatest(
     (select coalesce(max(id), 0) from outbox), (select coalesce(max(id), 0) from outbox_retry),
     (select coalesce(max(id), 0) from outbox_parked), (select coalesce(max(id), 0) from outbox_dead), 1));
   ```

5. Починка: удалите все партиции (включая хвост), тики, курсоры и маркеры удаления, которые описывают старый счётчик
   xid, затем маркер остановки.

   ```sql
   do $$
   declare p regclass;
   begin
     for p in select inhrelid::regclass from pg_inherits where inhparent = 'outbox'::regclass loop
       execute format('drop table %s', p);
     end loop;
   end $$;
   delete from outbox_tick;
   delete from outbox_cursor;
   delete from outbox_dropping;
   delete from outbox_halt;  -- тики и курсоры починены на этом шаге
   ```

6. Запустите один инстанс. Его bootstrap создаст диапазоны выше текущего xid, тик и по одному курсору на шард, и его
   relay запустится.
7. Отправьте сохранённые строки заново, затем удалите промежуточную таблицу. Строки получают новые id и xid и
   сохраняют свои message id, поэтому потребители дедуплицируют те из них, что были доставлены после снятия бэкапа, в
   пределах своего окна inbox:

   ```sql
   insert into outbox (shard, message_id, topic, key, payload, headers)
   select shard, message_id, topic, key, payload, headers from outbox_unrelayed order by created_at, id;
   drop table outbox_unrelayed;
   ```

   Сортировка сначала по `created_at`: после логического переключения id нового кластера начинаются заново ниже
   старых, а `created_at` сообщений одного ключа на каждом сервере следует порядку их коммитов.

   Сделайте это до того, как сервис начнёт принимать трафик (держите первый инстанс вне балансировщика), иначе новые
   сообщения тех же ключей могут уйти первыми. Затем запустите остальные инстансы.

Inbox это не затрагивает: он партиционирован по времени сообщения, а не по xid.

### Репликация, failover и PITR

- **Асинхронная репликация.** После failover у нового primary может не оказаться коммитов, чьи сообщения relay уже
  доставил: потребители видели события, данных которых больше нет (фантомы). Где это важно, используйте синхронную
  репликацию.
- **PITR или восстановление физического бэкапа на более раннюю точку.** Коммиты после этой точки пропадают вместе со
  своими сообщениями; уже доставленные из них — фантомы. Курсоры возвращаются туда, где были в этой точке, поэтому
  сообщения, прошедшие relay после неё, доставляются снова: это дубликаты, которые inbox отбрасывает в пределах своего
  окна.

### Решардинг

`outbox.shards` определяет, в каком шарде живёт каждый ключ. Старт падает, если значение отличается от числа шардов в
`<base>_cursor`, потому что уже записанные строки несут свой шард.

Процедура ещё не отрепетирована авторами на живой базе — прогоните её на копии (staging) перед использованием в продакшене.

1. Остановите пишущий трафик и дайте relay доставить всё: писатели остановлены, а retry-таблица и `outbox_parked`
   пусты (строки в них сохраняют старый шард; отложенные записи возвращаются по сроку, пока обработчик не перестанет
   их откладывать).
2. Остановите все инстансы.
3. При остановленных инстансах проверьте, что для relay ничего не осталось. Все пять запросов должны вернуть 0; если
   какой-то не вернул, снова запустите инстансы со старым `outbox.shards` и вернитесь к шагу 1.

   ```sql
   select count(*) from outbox_retry;
   select count(*) from outbox_parked;
   select count(*) from (select distinct on (shard) shard, target_tick_id from outbox_cursor
                          order by shard, seq desc) c
    where c.target_tick_id is not null;                          -- незавершённый батч
   select count(*) from outbox o
     join (select distinct on (shard) shard, tick_id from outbox_cursor order by shard, seq desc) c using (shard)
     join outbox_tick t on t.id = c.tick_id
    where o.xid >= pg_snapshot_xmin(t.snapshot)
      and not pg_visible_in_snapshot(o.xid, t.snapshot);         -- закоммиченные строки, которые не прошёл ни один курсор
   select count(*) from pg_class c where relname ~ '^outbox_x[0-9]+$' and relkind = 'r'
      and not exists (select 1 from pg_inherits where inhrelid = c.oid);  -- осиротевшие диапазоны: сначала восстановите их
   ```

4. `delete from outbox_cursor;`
5. Задеплойте с новым `outbox.shards`. Bootstrap создаст по одному курсору на шард на последнем тике.

Новые курсоры начинают с последнего тика, поэтому строка, не прошедшая relay к этому моменту, была бы пропущена или
прочитана под своим старым шардом: шаг 3 — это контрольная точка.

### Rolling-деплои

- **Держите `outbox.table` (и `outbox.inbox.table`) одинаковым и одинаково записанным на всех инстансах.** Новое имя —
  это новый outbox: сообщения, оставшиеся в старой таблице, обрабатывают только инстансы, которые всё ещё настроены на
  неё. Указывайте схему (`app.outbox`): имя без схемы означает таблицу в `current_schema()` каждого соединения
  (`db.schema` в Kora его меняет), и инстансы с разными search path использовали бы разные таблицы. Варианты записи,
  которые указывают на одну таблицу (`outbox`, `public.outbox`, `Outbox`), разделяют все блокировки. Старт падает, если
  имя без схемы указывает не на ту таблицу, которую он создал бы (таблица есть только в более поздней схеме
  search_path или её затеняет временная таблица).
- **`outbox.shards`:** новые инстансы с другим значением падают при старте; см. [Решардинг](#решардинг).
- **Уменьшение `retention` или изменение `partitionInterval` у inbox:** выкатывайте на все инстансы разом. Пока старые
  и новые инстансы работают бок о бок, старый может вставить id в диапазон, который новый только что удалил, и это
  валит транзакцию этого потребителя (SQLState 23514).
- **Переход на версию с `Outcome.requeue`.** Первый новый инстанс создаёт `<base>_parked` и один раз добавляет столбцы
  `<base>_cursor.parked_due_at` и `<base>_retry.requeues` (только метаданные; `ALTER` ждёт идущих шагов relay не
  больше 5 с, иначе старт падает, и его можно повторить). Пока рядом работают инстансы прежней версии, их строки
  курсора не несут `parked_due_at`, поэтому отложенная запись может ждать возвращения до `maintenanceInterval`. После
  отката на версию без requeue отложенные записи лежат в `<base>_parked` недоставленными, пока снова не запустится
  версия с ним.
- **Остановка** мягкая: воркеры перестают брать шарды, выполняющиеся вызовы обработчика получают
  `relay.shutdownTimeout`, затем прерываются, а их незакоммиченные батчи позже доставляются снова — другим инстансом
  или после рестарта (дубликаты, а не потери).

### Стоимость простоя

Простаивающий relay обходится примерно в `1 / idlePollInterval + shards / 5` пишущих транзакций в секунду на весь
кластер: примерно один idle-тик за `idlePollInterval` при любом числе инстансов и по одной строке курсора на шард раз в
5 с. Со значениями по умолчанию (200 мс, 32 шарда) намерено 11.4 transaction id в секунду. Простаивающие воркеры
также опрашивают базу двумя небольшими read-only запросами (последний тик, курсоры) примерно раз в
`idlePollInterval` каждый. Под нагрузкой шаг получают только шарды, в которые с предыдущего тика пришли строки, поэтому
при малом трафике число шардов стоит немного. В простое gauge лага, очистка тиков и удаление партиций отстают до 5 с. Увеличьте
`idlePollInterval`, чтобы простаивающий кластер обходился дешевле, ценой более медленного подхвата коммитов, сделанных
на других инстансах.

### Gauges после горячей перезагрузки конфигурации

Gauges регистрируются один раз на имя, и побеждает первая регистрация. Если ваше приложение перезагружает конфигурацию
на лету и Kora пересобирает компоненты outbox, gauges продолжают читать компоненты, созданные первыми, и замирают на
последних значениях. Счётчиков и таймеров это не касается. После изменения `outbox.*` перезапускайте приложение.

## Наблюдаемость

### Метрики

Метры Micrometer, активны, когда в графе есть `MeterRegistry`. В Prometheus к именам добавляются `_total`, `_seconds` и
т. д. (`outbox_messages_delivered_total`, `outbox_delivery_delay_seconds_bucket`).

| Метр | Тип | Теги | Значение |
|---|---|---|---|
| `outbox.messages.enqueued` | counter | `topic` | Отправленные сообщения, считаются в `send`, включая отправки, чья транзакция откатилась. Не вычисляйте отставание как enqueued − delivered; используйте `outbox.shard.lag`. |
| `outbox.messages.delivered` | counter | `topic` | Записи, о доставке которых сообщил обработчик. |
| `outbox.messages.failed` | counter | `topic`, `outcome` = `retry` / `dead` | Записи, отведённые в retry-таблицу (или снова не прошедшие в ней), и записи, ушедшие в dead letters. |
| `outbox.messages.requeued` | counter | `topic` | Записи, отложенные через `Outcome.requeue()`: каждый раз, включая повторные. `requeue` сверх `maxRequeues` считается в `failed{outcome="dead"}`. |
| `outbox.delivery.delay` | timer, percentile histogram | `topic` | От вставки строки до outcome «доставлено». Бакеты от 1 мс до 6 ч, чтобы сбои оставались видны в p99; около 110 серий бакетов на топик. |
| `outbox.batch.duration` | timer, percentile histogram | — | Один шаг шарда, записавший строку курсора (прогресс или пауза). |
| `outbox.shard.paused` | counter | — | Паузы шардов (цель недоступна, сбойная голова, нет свободного потока обработчика). |
| `outbox.shard.lag` | gauge, с | — | Насколько тик самого медленного шарда отстаёт от последнего тика. |
| `outbox.retry.size`, `outbox.parked.size`, `outbox.dead.size` | gauges | — | Число строк в retry-таблице, среди отложенных записей (`<base>_parked`) и в dead-таблице. |
| `outbox.partitions.premade` | gauge | — | Диапазоны выше текущего xid. |
| `outbox.partitions.overflow` | gauge | — | 1, пока используется хвост («tail overflow»). |
| `outbox.partitions.xids_left` | gauge | — | Сколько xid осталось до того, как вставки начнут падать, пока хвост не присоединён; NaN, пока присоединён. |
| `outbox.partitions.orphaned` | gauge | — | Отсоединённые диапазоны, сохранённые, потому что в них есть строки. |
| `outbox.maintenance.transaction_pooling` | gauge | — | 1, если последний запуск обнаружил transaction pooler. |
| `outbox.maintenance.since_success` | gauge, с | — | Время с последнего завершённого запуска обслуживания на любом инстансе (по часам базы). |
| `outbox.inbox.outside_window` | counter | — | Id, принятые без дедупликации (вне окна или не UUIDv7). |
| `outbox.inbox.maintenance.transaction_pooling`, `outbox.inbox.partitions.orphaned` | gauges | — | То же для inbox. |

Gauges outbox обновляет обслуживание каждые `maintenanceInterval` на инстансе, который его выполняет; инстанс, чей
запуск обнаружил, что блокировку обслуживания держит другой, сохраняет прежние значения (NaN до своего первого
запуска). `since_success` читает каждый инстанс.

Логи (SLF4J, `io.github.dsudomoin.kora.outbox`): INFO — старт, остановка и DDL партиций; WARN — отведённые ключи, паузы
шардов, отложенные расширения и устаревшее обслуживание; ERROR — dead letters, ошибки в обработчиках, остановка из-за
регрессии xid, tail overflow, `premade < 2`, отсоединённый хвост, сироты и transaction pooling. Повторяющиеся сообщения
ограничены по частоте.

### Трассировка

С `Tracer` в графе:

1. `send` сохраняет трассу писателя (`traceparent`, `tracestate`) в заголовках сообщения.
2. Relay начинает один span на каждую попытку доставки, **`outbox deliver <topic>`**, kind `PRODUCER`, дочерний к
   трассе писателя, с атрибутами `messaging.destination.name`, `outbox.message_id`, `outbox.attempt`,
   `outbox.delay_ms` и `outbox.outcome` (`delivered`, `retry`, `permanent`, `not_attempted`, `requeued`); сбои
   записываются в него.
3. Обработчик получает запись, в которой заголовки трассы заменены на заголовки этого span. Обработчик Kafka пересылает
   их, поэтому Kafka-потребитель на Kora продолжает ту же трассу; `handleBatch` по умолчанию выполняет `handle` в
   контексте Kora, несущем этот span, поэтому клиенты Kora, вызванные из него, тоже продолжают трассу.

С parent-based сэмплером (по умолчанию в OpenTelemetry) span-ы доставки сэмплируются, если была сэмплирована трасса
писателя. Несэмплированные span-ы всё равно создаются на каждую запись — небольшая цена на потоке relay.

### Локальный стенд

В `observability/` лежит локальный стенд для бенчмарк-приложения: Postgres 16 (2 CPU, 4 ГБ), Kafka, Prometheus,
Grafana, Tempo и postgres-exporter.

```bash
docker compose -f observability/docker-compose.yml up -d
```

Grafana на http://localhost:3000 открывает дашборд **Outbox** (uid `outbox`): пропускная способность, задержка
доставки, лаг шардов, сбои, retry- и dead-таблицы, партиции, панели Postgres и потребителя. Трассы — в Explore → Tempo
(http://localhost:3200, OTLP gRPC на `localhost:4317`); Prometheus на http://localhost:9090 собирает метрики
бенчмарк-приложения с `host.docker.internal:8085`. Чтобы использовать дашборд со своим сервисом, импортируйте
`observability/grafana/dashboards/outbox.json`. Подробности: `observability/README.md`.

## Тестирование вашего сервиса

- **Юнит-тесты.** `Outbox` и `Inbox` — интерфейсы: мокайте их. `TypedOutbox` принимает в конструкторе `Outbox` и
  `OutboxSerializer`.
- **Интеграционные тесты** запускают настоящий модуль на Postgres 16 (Testcontainers). Выключите фоновый relay и
  доставляйте синхронно через `OutboxRelay.drain()`: он делает тик, затем обрабатывает каждый шард, пока все шарды не
  дойдут до этого тика и не останется голов в retry-таблице и отложенных записей, срок которых наступил, и возвращает
  число доставленных сообщений. Он работает независимо от того, запущен ли relay. Он бросает
  `IllegalStateException`, если ни один шард не продвинулся за 2 × `batchTimeout`, хотя работа осталась (например,
  шард приостановлен из-за сбойного обработчика), или пока relay остановлен.

```java
@KoraAppTest(App.class)
class OrdersTest implements KoraAppTestConfigModifier {

    // Testcontainers 2.x (org.testcontainers.postgresql / .kafka); в 1.x те же контейнеры лежат в других пакетах
    static final PostgreSQLContainer PG = new PostgreSQLContainer("postgres:16-alpine");
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.1");

    static {
        PG.start();
        KAFKA.start();
    }

    @TestComponent
    private Orders orders;

    @TestComponent
    private OutboxRelay relay;

    @Override
    public KoraConfigModification config() {
        return KoraConfigModification.ofString("""
                db { jdbcUrl = "%s", username = "%s", password = "%s", poolName = "test" }
                outbox {
                  relay.enabled = false
                  partitions { xidsPerPartition = 1000000, premake = 4 }
                  kafka.driverProperties { "bootstrap.servers" = "%s" }
                }
                """.formatted(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword(), KAFKA.getBootstrapServers()));
    }

    @Test
    void createSendsOneMessage() {
        orders.create(new Order(1, "u1"));

        assertThat(relay.drain()).isEqualTo(1); // подтверждено Kafka
        // затем прочитайте топик "orders" через KafkaConsumer и проверьте запись и её outbox-message-id
    }
}
```

- `drain()` останавливается, когда не осталось голов в retry-таблице и отложенных записей, срок которых наступил:
  запись, чья отправка упала и которая ждёт своего backoff, ещё не доставлена; запись, отложенная через `requeue` до
  своего срока, — тоже.
  Создайте топик до теста и дополнительно проверяйте, что `select count(*) from outbox_retry` = 0 (а с обработчиком,
  который откладывает записи, — и `outbox_parked`). С меньшим `batchTimeout` `drain()` быстрее сдаётся при тестовом
  обработчике, запоминающем записи; с Kafka значение должно оставаться ≥ `delivery.timeout.ms + 5 s + max.block.ms`.
- `partitions { xidsPerPartition = 1000000, premake = 4 }` ускоряет bootstrap новой таблицы (6 партиций вместо 66).
- Чтобы проверять доставленное без Kafka, дайте тестовому графу собственный обработчик: тестовый `@KoraApp`, фабрика
  `OutboxHandler` которого возвращает обработчик, запоминающий записи, заменяет обработчик Kafka по умолчанию (с
  `KafkaOutboxModule` действует правило из раздела [Свой обработчик](#свой-обработчик)).
- С `relay.enabled = true` доставка асинхронная (вскоре после коммита); опрашивайте результат, а не ждите через sleep.
- Массовым тестам с множеством различных ключей нужен увеличенный `max_locks_per_transaction` в тестовом контейнере
  (`withCommand("postgres", "-c", "max_locks_per_transaction=256")`).
- Используйте уникальный `outbox.table` для каждого тестового класса, если тесты делят одну базу и не должны видеть
  сообщения друг друга.

## Бенчмарки

Полные результаты с методикой, всеми окнами измерений и оговорками: [`bench/RESULTS.md`](bench/RESULTS.md).
Бенчмарк-приложение и его сценарии: [`bench/README.md`](bench/README.md).

**Стенд.** Apple M5 Pro, 18 ядер (6 + 12), 48 ГБ RAM, macOS 27.0.1. ВМ Docker Desktop с 18 CPU и 7.7 ГБ. Postgres
16.15 в контейнере, ограниченном **2 CPU и 4 ГБ** без swap (`cpus: 2`, `mem_limit` = `memswap_limit` = 4g),
`shared_buffers = 1GB`, `max_connections = 300`, `max_locks_per_transaction = 256`, с надёжной записью
(`synchronous_commit`, `fsync` и `full_page_writes` включены). Kafka 3.9.1, один брокер KRaft. Приложение — на JVM
хоста (Temurin 21, Kora 1.2.23).

**Основная нагрузка:** 2 потока-писателя × 10 000 сообщений на транзакцию (`sendAll`, у каждого сообщения случайный
ключ), payload 100 байт, 4 relay-воркера, `batchSize = 20000`, `idlePollInterval = 100ms`.

| шарды | обработчик | доставлено, сообщ./с | задержка p50 / p99 |
|---|---|---|---|
| 1 | noop | 273 427 | 0.078 / 0.389 с |
| 2 | noop | 279 656 | 0.049 / 0.112 с |
| 4 | noop | 259 663 | 0.036 / 0.110 с |
| 2 | Kafka | 273 143 | 0.065 / 0.150 с |
| 4 | Kafka | 263 829 | 0.051 / 0.122 с |
| 4 | noop, relay с настройками библиотеки по умолчанию | 271 259 | 0.086 / 0.219 с |
| 1 | Kafka | 202 685 (записано 279 770) | 47.9 / 67.8 с: режим не установился |

**Об оборудовании.** Числа сняты на ядрах Apple M5 Pro, база получает квоту Docker в 2 CPU; ядро M5 Pro вполне может
быть быстрее облачного, и на облачной ВМ с её диском результаты будут другими. Брокер Kafka — один узел с replication
factor 1, поэтому `acks=all` не ждёт ни одного follower. Один шард с relay в Kafka не успевает за этой нагрузкой
(шард обрабатывается одним воркером последовательно); используйте 2 шарда или больше.

Другие результаты на том же стенде:

- **Длинная транзакция:** при 120 000 сообщ./с (bulk, без бизнес-таблиц) и транзакции, открытой 300 с, на всём
  протяжении доставлено = записано, а p99 вырос с 0.053 с до 0.067 с. Relay, который читает только ниже snapshot
  xmin, по построению простоял бы все 300 с (не измерялось). В режиме заказов (4 000 сообщ./с на базе, уже
  загруженной на 200 % CPU, транзакция открыта 600 с) тоже ничего не встало, но p99 вырос с 0.22 с до 0.61 с: в
  бизнес-таблицах накопились мёртвые версии строк, и замедлилась вся база, включая запросы outbox.
- **Одно сообщение на транзакцию:** база с 2 CPU коммитит около 6 800 транзакций заказа в секунду (вставка заказа,
  обновление счётчика пользователя и один `send` в каждой); с inbox-потребителем на той же базе закоммичено 5 650
  сообщ./с, задержка p99 0.43 с. Это на локальном SSD, где fsync дешёвый; на сетевом томе коммитов будет меньше.
- **Малая нагрузка и число шардов:** на 100 сообщ./с (одно сообщение на транзакцию, Kafka) 32 шарда доставляли с
  задержкой p50 / p99 34 / 110 мс при 20 % CPU базы, один шард — 31 / 105 мс и 10 %, 128 шардов — 36 / 113 мс. До того
  как тики стали записывать шарды, 32 шарда давали 42 / 121 мс и 46 % CPU.
- **Потребитель с inbox:** с `tryAcceptAll` на каждый poll потребитель, работая на базе в одиночку, потреблял
  216 792 сообщ./с против 26 040 сообщ./с с одним `tryAccept` на запись.
- **Стоимость для писателя по сравнению с простой вставкой** (pgbench, та же база): на 10.5 % меньше сообщений в
  секунду при 1 сообщении на транзакцию; на 27.5 % больше времени на транзакцию из 10 000 различных ключей, и вся
  разница — advisory-блокировки по ключам (около 0.74 мкс на ключ), на которых и держится порядок коммитов по ключу.
- **Сбои:** 60-секундная недоступность Kafka при 4 000 сообщ./с (отставание выбрано через 6 с после возвращения Kafka)
  и `kill -9` приложения ничего не потеряли (SQL-сверка по каждой закоммиченной строке).
- **Хаос, часть 1** (прогон на промежуточной версии библиотеки): 3 инстанса, убитые `kill -9` 18 раз за
  15 мин (2 465 656 заказов), рестарт Postgres, 30-секундная пауза Postgres, рестарт Kafka, пул на 12 соединений для 52
  потоков, каждому из которых нужно соединение, и обработчик с 20 мс на запись. В каждом прогоне: потеряно 0, фантомов
  (доставленных откаченных заказов) 0, нарушений порядка 0; дубликатов не больше 0.03 %. С тех пор код менялся в
  обслуживании партиций и пуле потоков обработчика; часть 1 на финальном коде не перезапускалась.
- **Хаос, часть 2** (прогон на версии со всеми исправлениями): частая смена партиций с 12 `kill -9` во время DDL партиций (1 660 344
  заказа; 6 диапазонов, оставшихся отсоединёнными, были убраны примерно за секунду), повторный прогон с убийством
  3 инстансов (2 476 828 заказов) и 60-минутный soak при 2 000 заказов/с плюс 20 000 bulk-сообщений/с (7 045 905
  заказов, 71.9 M bulk-сообщений). В каждом прогоне: потеряно 0, фантомов 0, нарушений порядка 0. В soak heap,
  соединения и таблицы тиков, курсоров и retry оставались ровными, но p99 задержки доставки вырос с 0.14 с до 0.34 с
  (худшее окно 1.6 с), пока база выросла до 12 ГБ на стенде с 4 ГБ памяти; собственное время батча у relay
  оставалось ровным. Анализ — в `bench/RESULTS.md`.

Графики Grafana с дашборда **Outbox** из `observability/` (время на них московское, UTC+3; пунктирные линии —
аннотации сценариев и сбоев; все восемь скриншотов — в [`bench/RESULTS.md`](bench/RESULTS.md)):

![max: 362 900 msg/s](docs/images/benchmarks/00-max-362k-msg-per-s.png)

*max: 4 писателя × 10 000 сообщений на транзакцию, шардов 4, обработчик noop, без консьюмера. Линии committed и
delivered совпадают на плато ~360–380k msg/s (362 900 в окне 04:30:34–04:33:34), p99 задержки доставки ~0.13 с,
retry и dead пусты. Прогон на более ранней версии кода, до финальных исправлений.*

![bulk на финальном коде: ~253k msg/s](docs/images/benchmarks/01-bulk-250k-msg-per-s.png)

*bulk на финальном коде (повторная проверка): 2 писателя × 10 000 сообщений на транзакцию, шардов 4, без
консьюмера; слева обработчик noop, справа Kafka. committed и delivered совпадают на ~250k msg/s (252 914 и 253 427 в
окнах по 180 с), p99 задержки доставки ~0.12–0.13 с, retry и dead пусты.*

![хаос: 3 инстанса, 19 kill -9](docs/images/benchmarks/05-chaos-3-instances-kill9.png)

*Хаос, часть 2 (финальный код): 3 инстанса по 1 000 заказов/с, 19 `kill -9` случайного инстанса за 15 мин (пары
пунктиров — убийство и рестарт). Коммиты проседают при каждом убийстве и восстанавливаются, всплески consumed —
консьюмеры выбирают накопившееся, p99 задержки доставки 0.1–0.25 с, retry и dead пусты; сверка: потеряно 0,
фантомов 0, нарушений порядка 0.*

![soak на финальном коде: 1 час](docs/images/benchmarks/07-soak-1h.png)

*Soak на финальном коде (диагностический повтор soak части 2): 1 ч, 2 000 заказов/с и 20 000 bulk-сообщений/с на
двух инстансах. committed, delivered и consumed идут одной ровной линией ~22k msg/s весь час, p99 задержки доставки
~0.15–0.2 с без роста к концу, retry и dead пусты: рост хвоста задержки из части 2 здесь не повторился.*

## Заметки о дизайне

- **Журнальная структура, только вставки.** Таблицы outbox, тиков и курсоров на горячем пути получают только вставки.
  Прогресс — это новая строка курсора на каждый шаг шарда, а не update, и читатели всегда берут самую новую строку.
  Старые тики и строки курсоров удаляет обслуживание с холодного конца, а пройденные строки outbox уходят вместе со
  своей партицией. Горячий путь не проходит по цепочкам мёртвых версий строк, поэтому длинная транзакция в другом
  месте базы его не стопорит (он всё равно делит базу, которая целиком замедляется, когда в нагруженных таблицах
  копятся мёртвые версии).
- **Тики-снапшоты.** Тик записывает `pg_current_snapshot()`. Батч шарда между тиками P и C — это ровно те строки, чей
  xid виден в C и не виден в P: транзакции, закоммиченные между двумя тиками. Тики делаются под блокировкой,
  удерживаемой до коммита, поэтому порядок тиков — это порядок коммитов; внутри батча строки идут в порядке id, а
  advisory-блокировка по ключу, взятая до назначения id, делает порядок id равным порядку коммитов для каждого ключа.
  Тики делаются сразу после локального коммита (пробуждение после коммита, не чаще одного раза в `minTickInterval`), а
  в остальное время — каждые `idlePollInterval`. Каждый тик ещё записывает, в каких шардах появились строки с
  предыдущего тика (те же две индексные пробы на шард, что и в запросе батча), и relay обрабатывает только эти шарды;
  шард без новых строк получает шаг раз в 5 с, чтобы его курсор двигался для обслуживания. Проба стоит около 0,45 мкс,
  а вторая идёт по каждой транзакции кластера (любой базы), завершившейся с прошлого тика, в тех шардах, где первая
  ничего не нашла, и всё это под блокировкой тиков: при 32 шардах запись тика занимает 0,13 / 1,6 / 5,9 мс при 0 / 100 /
  400 таких транзакциях, при 128 шардах 0,16 / 6,4 / 23,9 мс.
- **Партиции-диапазоны по xid8.** Таблица партиционирована по xid пишущей транзакции. Диапазон удаляется, когда xmin
  тика самого медленного шарда выше него: каждая транзакция в нём к этому тику завершилась, а закоммиченные прошли
  relay. Правило точное и не требует часов. Хвост принимает строки, когда обслуживание отстаёт.
- **Шарды.** Ключи хешируются в шарды; шард одновременно обрабатывает один воркер по всем инстансам (транзакционная
  advisory-блокировка), а упавший или зависший воркер теряет шард, когда Postgres завершает его сессию.
- **В отличие от отсечки по xmin.** Многие outbox-relay читают только строки ниже xid самой старой выполняющейся
  транзакции: это безопасно, но всё стоит, пока открыта хоть одна такая транзакция, и доставка идёт в порядке xid, а
  не в порядке коммитов. Здесь запрос батча берёт «транзакции, начатые после P» плюс «транзакции, выполнявшиеся в
  момент P и завершённые к C», поэтому длинная транзакция задерживает только свои строки, а порядок следует коммитам.

## Известные ограничения

- Один relay (один набор курсоров, один обработчик) на таблицу outbox; раздавайте сообщения по разным целям через
  `byTopic` или дальше по цепочке.
- Решардинг требует пустого outbox. Tail overflow и регрессия xid требуют вмешательства оператора.
- Сервис, чей DataSource обязан ходить через пулер в режиме transaction, пока не может использовать библиотеку.
- Один шард обрабатывается последовательно (чтение, отправка, ожидание ack, коммит): один горячий шард ограничивает
  пропускную способность; используйте больше шардов.
- Если зависшая запись находится глубоко среди последующих записей ключа в retry-таблице, этот ключ разгребается
  примерно по одной строке за шаг.
- Одна транзакция с миллионами строк outbox доставляется корректно, но медленно.
- Inbox дедуплицирует только в пределах своего окна и своего scope.
- Защиты от failover при асинхронной репликации нет (фантомные сообщения).

## Скиллы для AI-агентов

В каталоге [`skills/`](skills) лежат скиллы в открытом формате [Agent Skills](https://agentskills.io/specification):
инструкции, по которым кодовый агент (Claude Code, Codex, Cursor и другие) правильно использует библиотеку в сервисе
на Kora. Они дают агенту то, чего нет в javadoc: бюджет вызова обработчика, правило одного `sendAll` на транзакцию,
подготовку Postgres и топиков Kafka, проверки в тестах и алерты. Тексты скиллов написаны по-английски.

| Скилл | Когда агент его загружает |
|---|---|
| `using-kora-outbox` | Подключение, настройка и отправка; роль в базе, соединения, топики Kafka перед деплоем |
| `writing-kora-outbox-handlers` | Свой `OutboxHandler`, `byTopic`, `batchSize` и `batchTimeout` под медленную цель, dead letters, `Outcome.requeue` |
| `consuming-with-kora-outbox-inbox` | Потребитель с `Inbox`: дедупликация, scope, записи без заголовка `outbox-message-id` |
| `testing-kora-outbox` | Интеграционные тесты с Testcontainers и `OutboxRelay.drain()`, тесты, не проверяющие доставку |
| `operating-kora-outbox` | Метрики, алерты, dead letters, процедуры восстановления, решардинг, деплои |

**Claude Code.** Репозиторий — маркетплейс с одним плагином `kora-outbox`:

```
/plugin marketplace add dsudomoin/kora-transactional-outbox
/plugin install kora-outbox@kora-transactional-outbox
```

**Другие агенты.** Скопируйте каталоги из `skills/` в каталог скиллов агента. Codex и Cursor читают
`~/.agents/skills/` (для всех ваших проектов) и `.agents/skills/` в репозитории сервиса (для всех, кто с ним
работает); Cursor также читает `.cursor/skills/` и `.claude/skills/`, а Claude Code без плагина — `~/.claude/skills/`
и `.claude/skills/`.

```bash
git clone https://github.com/dsudomoin/kora-transactional-outbox
mkdir -p ~/.agents/skills
cp -R kora-transactional-outbox/skills/* ~/.agents/skills/
```

## Лицензия

MIT, см. [LICENSE](LICENSE).
