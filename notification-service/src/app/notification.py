from flask import Flask
from kafka import KafkaConsumer
import threading, json, smtplib, os
from email.message import EmailMessage
from kafka.errors import NoBrokersAvailable
import time

app = Flask(__name__)

# SUSCRÍBETE al topic correcto
KAFKA_TOPIC = 'order-validated'
KAFKA_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

SMTP_SERVER   = os.getenv('SMTP_SERVER')
SMTP_PORT     = int(os.getenv('SMTP_PORT', 2525))
SMTP_USER     = os.getenv('SMTP_USER')
SMTP_PASSWORD = os.getenv('SMTP_PASSWORD')

def send_email(to_email, subject, body):
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From']    = SMTP_USER
    msg['To']      = to_email
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
    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_SERVER],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id='notification-group'
            )
            print("▶️ Connected to Kafka, listening on", KAFKA_TOPIC)
            break
        except NoBrokersAvailable:
            print("⚠️ Kafka no disponible aún, reintentando en 5s…")
            time.sleep(5)

    for msg in consumer:
        event = msg.value
        if event.get("type") != "order_validated":
            continue

        data = event["data"]
        order_id      = data.get("order_id")
        to_email      = data.get("customer_email")
        if not order_id or not to_email:
            print(f"⚠️ Datos insuficientes en evento: {data}")
            continue

        print(f"📥 Pedido #{order_id} listo para notificar a {to_email}")
        subject = f"Confirmación de Pedido #{order_id}"
        body    = (
            f"Hola,\n\n"
            f"Tu pedido #{order_id} ha sido confirmado y el stock ha sido reservado.\n"
            "Gracias por tu compra."
        )
        #send_email(to_email, subject, body)

@app.route('/')
def index():
    return "✅ Notification Service activo."

threading.Thread(target=listen_to_kafka, daemon=True).start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
