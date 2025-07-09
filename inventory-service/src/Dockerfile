FROM python:3.11-slim
WORKDIR /app

# Instala dependencias
COPY src/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia código y entrypoint
COPY src/app ./src/app
COPY src/entrypoint.sh .

# Dale permiso de ejecución
RUN chmod +x entrypoint.sh

EXPOSE 8001
ENTRYPOINT ["./entrypoint.sh"]
