from confluent_kafka import Consumer, KafkaException, TopicPartition


import json
import sys
import time
import logging


logging.basicConfig(
    level=logging.DEBUG,
    filename='BatchMessageConsumer.log',
    encoding='utf-8',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Получаем идентификатор консьюмера из аргументов
consumer_id = sys.argv[1] if len(sys.argv) > 1 else "default_consumer"

# Минимальная пачка сообщений за один poll
MIN_BATCH_SIZE = 10

# Конфигурация консьюмера
conf = {
    'bootstrap.servers': 'localhost:9092, localhost:9093, localhost:9094', 	    # Используем IP брокера (для примера localhost)
    'group.id': 'test-group2',  # Группа для првого потребителя.
    'auto.offset.reset': 'earliest', # Чтение с самого первого доступного сообщения
    'fetch.min.bytes': 1024, # fetch.min.bytes — брокер ждёт накопления данных
    'fetch.wait.max.ms': 500, # fetch.max.wait.ms — максимальное ожидание данных от брокера fetch.wait.max.ms вместо fetch.max.wait.ms
    'enable.auto.commit': False, # Ручной коммит
#    'max.poll.records': 15, # Максимальное количество сообщений, возвращаемых за один вызов poll()
    'partition.assignment.strategy': 'roundrobin'
}

def on_assign(consumer, partitions):
#    """Callback при назначении новых партиций потребителю."""
    print(f"\n[{consumer_id}] --- Назначены новые партиции ---")
    for p in partitions:
        print(f"[{consumer_id}] Топик: {p.topic}, Партиция: {p.partition}")
    
    # Можно вручную установить offset
    # consumer.seek(partitions[0])

def on_revoke(consumer, partitions):
#    """Callback при отзыве партиций у потребителя."""
    print(f"\n[{consumer_id}] --- Отозваны партиции ---")
    for p in partitions:
        print(f"[{consumer_id}] Топик: {p.topic}, Партиция: {p.partition}")
    
    # Фиксация offset перед потерей партиций
    if partitions:
        consumer.commit(offsets=partitions)
        print(f"[{consumer_id}] Offsets зафиксированы.")
def process_batch(self,batch):
        offsets = [] # Cоздания пустого списка offsets
        for msg in batch:
            try:
                key = msg.key().decode('utf-8') if msg.key() else None
                raw_value = msg.value().decode('utf-8') if msg.value() else None
                try:
                    value = json.loads(raw_value) if raw_value else None
                except json.JSONDecodeError as e:
                    # Добавление ошибки в лог
                    debug_string = "Ошибка декодирования JSON!"
                    logging.debug(debug_string)
                    debug_string = f"Сообщение об ошибке: {e.msg}"
                    logging.debug(debug_string)
                    print("Ошибка декодирования JSON!")
                    print(f"Сообщение об ошибке: {e.msg}")
                # Вывод информации с идентификатором консьюмера
                print(f"\n[{consumer_id}] --- Получено сообщение ---")
                print(f"[{consumer_id}] Топик: {msg.topic()}")
                print(f"[{consumer_id}] Партиция: {msg.partition()}")
                print(f"[{consumer_id}] Смещение: {msg.offset()}")
                print(f"[{consumer_id}] Ключ: {key}")
                print(f"[{consumer_id}] Значение: {value}")

            except Exception as e:
                # Добавление ошибки в лог
                debug_string = f"[batch-consumer] ошибка десериализации: {e}"
                logging.debug(debug_string)
                print(f"[batch-consumer] ошибка десериализации: {e}")
            offsets.append(TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1))
            # Один коммит для всей пачки
        consumer.commit(offsets=offsets)
        print(f"[batch-consumer] закоммичена пачка из {len(batch)} сообщений")

# Создание консьюмера
consumer = Consumer(conf)

# Подписка с callback-функциями
consumer.subscribe(['test_topic_1'], on_assign=on_assign, on_revoke=on_revoke)

print(f"[{consumer_id}] Консьюмер запущен и ожидает сообщения...")
#    """
#    Основной цикл BatchMessageConsumer.
#
#    Явно вызывает poll() и накапливает сообщения во внутреннем буфере,
#    пока не наберётся MIN_BATCH_SIZE штук, после чего:
#      1. Обрабатывает все сообщения в цикле.
#      2. Коммитит оффсет ровно один раз для всей пачки.
#    """
batch = []  # внутренний буфер для накопления пачки
try:
    while True:
        msgtest = consumer.poll(1.0)
        if msgtest is None:
            continue
        if msgtest.error():
            # Добавление ошибки в лог
            debug_string = f"[batch-consumer] ошибка: {msgtest.error()}"
            logging.debug(debug_string)
            print(f"[batch-consumer] ошибка: {msgtest.error()}")
            continue
        batch.append(msgtest)
        if len(batch) >= MIN_BATCH_SIZE:
            process_batch(consumer, batch)
            batch.clear()
except KeyboardInterrupt:
    # Если в буфере остались необработанные сообщения — обрабатываем их
    if batch:
        print(f"\n[BatchConsumer] Обрабатываю остаток: {len(batch)} сообщ.")
        process_batch(consumer, batch)
        consumer.commit(asynchronous=False)
    print("\n[BatchConsumer] Остановлен пользователем.")
finally:
    consumer.close()
    print(f"[{consumer_id}] Консьюмер остановлен")

