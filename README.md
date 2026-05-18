# Kafka: Продюсер + два консьюмера (Python 3)

Практическая работа по Apache Kafka — Яндекс Практикум, Спринт 1.

## Описание классов

### `message.py` — `Message`
Датакласс, представляющий одно сообщение.  
Поля: `id` (UUID), `text`, `timestamp`, `source`.  
Методы:
- `Message.create(text, source)` — фабричный метод, генерирует id и timestamp.
- `serialize() → bytes` — объект → JSON → UTF-8 bytes.
- `Message.deserialize(bytes) → Message` — обратное преобразование.

### `producer.py` — Kafka-продюсер
Отправляет 30 сообщений в топик `practice-topic`.  
Гарантия доставки **At Least Once**: `acks="all"`, `retries=5`.  
Метод `send()` асинхронный; `flush()` и `close()` гарантируют отправку всего буфера.

### `single_consumer.py` — `SingleMessageConsumer`
Читает **по одному сообщению** за `poll()` (`max_poll_records=1`).  
Оффсет коммитится **автоматически** (`enable_auto_commit=True`).  
Принадлежит consumer group `single-consumer-group`.

### `batch_consumer.py` — `BatchMessageConsumer`
Накапливает в буфере минимум **10 сообщений** за несколько `poll()`.  
После накопления обрабатывает пачку в цикле и **один раз коммитит оффсет вручную** (`consumer.commit(asynchronous=False)`).  
Принадлежит независимой consumer group `batch-consumer-group`.

---

## Структура проекта

```
kafka_project/
├── docker-compose.yml   # Kafka-кластер из 3 брокеров (KRaft)
├── requirements.txt     # kafka-python
├── message.py           # Класс сообщения
├── producer.py          # Продюсер
├── single_consumer.py   # Консьюмер (по одному)
├── batch_consumer.py    # Консьюмер (пачками ≥10)
├── topic.txt            # Команда создания топика + describe
└── README.md
```

---

## Инструкция по запуску

### 1. Запуск кластера

```bash
docker compose up -d
```

Дождитесь, пока все три брокера поднимутся (10–20 секунд).

### 2. Создание топика (3 партиции, 2 реплики)

```bash

docker exec -it kafka-1 /bin/bash
kafka-topics --create \
  --bootstrap-server kafka-1:29092 \
  --topic test_topic_1 \
  --partitions 3 \
  --replication-factor 2
exit
```

Проверка:

```bash
docker exec -it kafka-1 /bin/bash
kafka-topics --describe \
   --topic test_topic_1 \
   --bootstrap-server kafka-1:29092
exit
```

### 3. Запуск консьюмеров (в отдельных терминалах)

**Терминал 2 — SingleMessageConsumer:**
```bash
python3 SingleMessageConsumer_log.py
```

**Терминал 3 — BatchMessageConsumer:**
```bash
python3 batch_consumer.py
```

### 5. Запуск продюсера

**Терминал 1:**
```bash
python3 Producer_log.py
```

---

## Как проверить, что всё работает

1. **Producer** — в терминале 1 должны появиться строки вида:
   ```
   Отправлено сообщение номер 1:
    ...
   Доставлено в Топик: test_topic_1 Партиция: [0] Смещение: 1
    ...
   ```

2. **SingleConsumer** — в терминале 2 каждое сообщение обрабатывается отдельно:
   ```
[default_consumer] Консьюмер запущен и ожидает сообщения...

[default_consumer] --- Назначены новые партиции ---
[default_consumer] Топик: test_topic_1, Партиция: 0
[default_consumer] Топик: test_topic_1, Партиция: 1
[default_consumer] Топик: test_topic_1, Партиция: 2

[1]

[default_consumer] --- Получено сообщение ---
[default_consumer] Топик: test_topic_1
[default_consumer] Партиция: 0
[default_consumer] Смещение: 1
[default_consumer] Ключ: synch_123
[default_consumer] Значение: {'data_1': 1, 'data_2': 'ОК'}

    ...

^C
[default_consumer] Прерывание пользователем
[default_consumer] Прерывание пользователем

[default_consumer] --- Отозваны партиции ---
[default_consumer] Топик: test_topic_1, Партиция: 0
[default_consumer] Топик: test_topic_1, Партиция: 1
[default_consumer] Топик: test_topic_1, Партиция: 2
[default_consumer] Offsets зафиксированы.
[default_consumer] Консьюмер остановлен

   ```

3. **BatchConsumer** — в терминале 3 сообщения обрабатываются пачками:
   ```
[default_consumer] Консьюмер запущен и ожидает сообщения...

[default_consumer] --- Назначены новые партиции ---
[default_consumer] Топик: test_topic_1, Партиция: 0
[default_consumer] Топик: test_topic_1, Партиция: 1
[default_consumer] Топик: test_topic_1, Партиция: 2

[default_consumer] --- Получено сообщение ---
[default_consumer] Топик: test_topic_1
[default_consumer] Партиция: 0
[default_consumer] Смещение: 1
[default_consumer] Ключ: synch_123
[default_consumer] Значение: {'data_1': 0, 'data_2': 'ОК'}

    ...

[default_consumer] --- Получено сообщение ---
[default_consumer] Топик: test_topic_1
[default_consumer] Партиция: 0
[default_consumer] Смещение: 10
[default_consumer] Ключ: synch_123
[default_consumer] Значение: {'data_1': 92, 'data_2': 'ОК'}
[batch-consumer] закоммичена пачка из 10 сообщений

[default_consumer] --- Получено сообщение ---
[default_consumer] Топик: test_topic_1
[default_consumer] Партиция: 0
[default_consumer] Смещение: 1973
[default_consumer] Ключ: synch_123
[default_consumer] Значение: {'data_1': 99, 'data_2': 'ОК'}
[batch-consumer] закоммичена пачка из 7 сообщений

[BatchConsumer] Остановлен пользователем.

[default_consumer] --- Отозваны партиции ---
[default_consumer] Топик: test_topic_1, Партиция: 0
[default_consumer] Топик: test_topic_1, Партиция: 1
[default_consumer] Топик: test_topic_1, Партиция: 2
[default_consumer] Offsets зафиксированы.
[default_consumer] Консьюмер остановлен


   ```

4. **Независимость групп** — оба консьюмера получают **все** сообщения,
   т.к. принадлежат разным consumer group.

---

## Принцип работы приложения

```
Producer ──push──► Kafka Топик (3 партиции, 2 реплики)
                          │
           ┌──────────────┴──────────────┐
           ▼                             ▼
  SingleConsumer                 BatchConsumer
  (group: test-group1)              (group: test-group2)
  max_poll_records=1             накопление ≥10 сообщений
  auto_commit=True               ручной commit после пачки
```

Каждая consumer group получает полный поток сообщений независимо.  
Партиционирование распределяет нагрузку между брокерами.  
Реплики обеспечивают отказоустойчивость при падении одного из брокеров.
