from confluent_kafka import Producer
import json
import socket
import logging
import time

logging.basicConfig(
    level=logging.DEBUG,
    filename='Producer.log',
    encoding='utf-8',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфигурация продюсера 
conf = {
    'bootstrap.servers': 'localhost:9092, localhost:9093, localhost:9094', 	    # Используем IP брокера (для примера localhost)
    'client.id': socket.gethostname(),          # Присваиваем продюсеру имя нашего хоста
    'acks': 'all',                              # Ждем подтверждения от всех реплик 
    'compression.type': 'none',                 # Можно изменить на 'gzip', 'snappy' и т.д.
    'retries': 5                                # Число попыток при ошибке
}

# Создаем  экземпляр продюсера, передаем ему конфигурацию 
producer = Producer(conf) 

# Callback-функция для обработки статуса доставки сообщений
def delivery_report(err, msg):
    if err is not None:
        # Вывод ошибки доставки
        print(f"Ошибка доставки: {err}")
        # Добавление ошибки в лог
        debug_string = f"Ошибка доставки: {err}"
        logging.debug(debug_string)
    else:
        # Сообщение успешно доставлено и подтверждено брокером
        print(f"Доставлено в Топик: {msg.topic()} Партиция: [{msg.partition()}] Смещение: {msg.offset()}")

# Функция асинхронной отправки сообщения
def produce_async(topic, headers, key, value):

    # JSON сериализация данных value
    try:
        serialized_value = json.dumps(value).encode('utf-8')
    except TypeError as e:
        # Добавление ошибки в лог
        debug_string = f"Ошибка типа данных: {str(e)}"
        logging.debug(debug_string)
        print(f"Ошибка типа данных: {str(e)}")
    except Exception as e:
        # Добавление ошибки в лог
        debug_string = f"Непредвиденная ошибка: {str(e)}"
        logging.debug(debug_string)
        print(f"Непредвиденная ошибка: {str(e)}")
    # Отправка сообщения
    producer.produce(
        topic=topic,               # Укажем топик
        key=key,                   # Укажем ключ
        value=serialized_value,    # Добавляем явно преобразованное значение
        headers=headers,           # Добавляем заголовки
        callback=delivery_report
    )

# Подготавливаем сообщение
message_topic = 'test_topic_1'
message_headers = [
    ("source", "python-producer"),
    ("version", "0.5"),
    ("content-type", "text/plain")
]
message_key = 'synch_123'
message_value = {'data_1': 123, 'data_2': 'ОК'}

# Отправляем сообщения
try:

    for i in range(100):
        message_value = {'data_1': i, 'data_2': 'ОК'}
        produce_async(message_topic, message_headers, message_key, message_value)
        print(f"Отправлено сообщение номер {i}")
        time.sleep(0.2)  # Задержка для наглядности
    
    # Завершаем работу: ждём, пока сообщения из буфера будут отправлены и обработаны колбэками.
    # Затем закрываем соединения и освобождаем ресурсы
    producer.flush()

except KeyboardInterrupt:
# Ctrl-C
    print("Прерывание пользователем")
    # Добавление ошибки в лог
    debug_string = "Прерывание пользователем"
    logging.debug(debug_string)
    print(f" Прерывание пользователем")

finally:
    producer.flush()
    debug_string = "Консьюмер остановлен"
    logging.debug(debug_string)
    print("Консьюмер остановлен")


