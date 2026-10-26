# Локальный стенд наблюдаемости

Postgres, Kafka, Prometheus, Grafana, Tempo и postgres-exporter для бенчмарк-приложения, которое запускается на хосте
(см. `bench/README.md`).

```bash
docker compose -f observability/docker-compose.yml up -d
```

| Что | Где |
|---|---|
| Grafana, домашний дашборд `Outbox`, анонимный admin | http://localhost:3000 |
| Prometheus (каждые 5 с собирает метрики бенчмарка с `host.docker.internal:8085` и postgres-exporter) | http://localhost:9090 |
| Tempo: приём OTLP gRPC, HTTP API | `localhost:4317`, http://localhost:3200 |
| Postgres `outbox`/`outbox`/`outbox`, 2 CPU, 4 ГБ без swap, `max_connections=300`, `max_locks_per_transaction=256`, загружен `pg_stat_statements` | `localhost:5432` |
| Kafka, single-node KRaft, автосоздание топиков выключено (бенчмарк сам создаёт `orders`) | `localhost:9092` |
| postgres-exporter, плюс `pg_outbox_partitions` и `pg_outbox_total_bytes` из `postgres-exporter/queries.yaml` | http://localhost:9187/metrics |

Все порты привязаны только к 127.0.0.1.

Prometheus (хранение 30 дней), Tempo (30 дней), Grafana и Postgres хранят данные в именованных томах
`kora-outbox-bench_*`, поэтому после `docker compose ... down` и затем `up -d` прошлые прогоны по-прежнему видны.
`down -v` их стирает. У Kafka тома нет: после `down` топик и offset-ы потребителей начинаются заново.

Маркеры сценариев — это аннотации Grafana с тегом `scenario` (отправляются POST-запросом в `/api/annotations`);
дашборд показывает их как слой `Scenarios`. Трассы `outbox-bench` — в Explore → Tempo.

Объём трасс: при `BENCH_TRACE_RATIO=1.0` 60-секундный прогон при 2 700 транзакций/с даёт около миллиона span и 150 МБ
в Tempo. Для долгих нагрузочных прогонов используйте низкую долю сэмплирования.
