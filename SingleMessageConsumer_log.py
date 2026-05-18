from confluent_kafka import Consumer, KafkaException
import json
import sys
import time
import logging

logging.basicConfig(
    level=logging.DEBUG,
    filename='SingleMessageConsumer.log',
    encoding='utf-8',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Получаем идентификатор консьюмера из аргументов
consumer_id = sys.argv[1] if len(sys.argv) > 1 else "default_consumer"

# Конфигурация консьюмера
conf = {
    'bootstrap.servers': 'localhost:9092, localhost:9093, localhost:9094', 	    # Используем IP брокера (для примера localhost)
    'group.id': 'test-group1',  # Группа для првого потребителя.
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'partition.assignment.strategy': 'roundrobin'
}

def on_assign(consumer, partitions):
    """Callback при назначении новых партиций потребителю."""
    print(f"\n[{consumer_id}] --- Назначены новые партиции ---")
    for p in partitions:
        print(f"[{consumer_id}] Топик: {p.topic}, Партиция: {p.partition}")
    
    # Можно вручную установить offset
    # consumer.seek(partitions[0])

def on_revoke(consumer, partitions):
    """Callback при отзыве партиций у потребителя."""
    print(f"\n[{consumer_id}] --- Отозваны партиции ---")
    for p in partitions:
        print(f"[{consumer_id}] Топик: {p.topic}, Партиция: {p.partition}")
    
    # Фиксация offset перед потерей партиций
    if partitions:
        consumer.commit(offsets=partitions)
        print(f"[{consumer_id}] Offsets зафиксированы.")

# Создание консьюмера
consumer = Consumer(conf)

# Подписка с callback-функциями
consumer.subscribe(['test_topic_1'], on_assign=on_assign, on_revoke=on_revoke)

print(f"[{consumer_id}] Консьюмер запущен и ожидает сообщения...")

try:
    batch = [] #создания пустого списка batch
    while True:
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaException._PARTITION_EOF:
                continue
            else:
                # Добавление ошибки в лог
                debug_string = f"[{consumer_id}] Ошибка сообщения: {msg.error()}"
                logging.debug(debug_string)
                print(f"[{consumer_id}] Ошибка сообщения: {msg.error()}")

        batch.append(msg) # Добавления в batch элемента msg с помощью метода append()
        print(f"\n[{len(batch)}]") # Тестовое сообщение для проверки обработки после 10 сообщений        
        
        # Десериализзация и обработка сообщения
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
            print("Ошибка декодирования JSON!", file=file_test)
            print(f"Сообщение об ошибке: {e.msg}", file=file_test)
        headers = {}
        if msg.headers():
            for header in msg.headers():
                headers[header[0]] = header[1].decode('utf-8')

        # Вывод информации с идентификатором консьюмера
        print(f"\n[{consumer_id}] --- Получено сообщение ---")
        print(f"[{consumer_id}] Топик: {msg.topic()}")
        print(f"[{consumer_id}] Партиция: {msg.partition()}")
        print(f"[{consumer_id}] Смещение: {msg.offset()}")
        print(f"[{consumer_id}] Ключ: {key}")
        print(f"[{consumer_id}] Значение: {value}")

except KeyboardInterrupt:
# Ctrl-C
    print(f"[{consumer_id}] Прерывание пользователем")
    # Добавление ошибки в лог
    debug_string = f"[{consumer_id}] Прерывание пользователем"
    logging.debug(debug_string)
    print(f"[{consumer_id}] Прерывание пользователем")

finally:
    consumer.close()
    debug_string = f"[{consumer_id}] Консьюмер остановлен"
    logging.debug(debug_string)
    print(f"[{consumer_id}] Консьюмер остановлен")

