from flask import Flask
from kafka import KafkaConsumer
import threading
import json
import smtplib
import os
from email.message import EmailMessage

app = Flask(__name__)

# Kafka
KAFKA_TOPIC = 'order_confirmed'
KAFKA_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

# SMTP config
SMTP_SERVER = os.getenv('SMTP_SERVER')
SMTP_PORT = int(os.getenv('SMTP_PORT', 587))
SMTP_USER = os.getenv('SMTP_USER')
SMTP_PASSWORD = os.getenv('SMTP_PASSWORD')

def send_email(to_email, subject, body):
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = SMTP_USER
    msg['To'] = to_email
    msg.set_content(body)

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)
            print(f"✅ Email enviado a {to_email}")
    except Exception as e:
        print(f"❌ Error al enviar email: {e}")

def listen_to_kafka():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_SERVER],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='notification-group'
    )
    for message in consumer:
        order = message.value
        print(f"📥 Pedido recibido para notificación: {order}")
        subject = f"Confirmación de Pedido #{order['order_id']}"
        body = f"Hola,\n\nTu pedido #{order['order_id']} ha sido confirmado.\nGracias por tu compra."
        send_email(order['customer_email'], subject, body)

@app.route('/')
def index():
    return "✅ Notification Service activo."

if __name__ == '__main__':
    threading.Thread(target=listen_to_kafka, daemon=True).start()
    app.run(host='0.0.0.0', port=8000)
