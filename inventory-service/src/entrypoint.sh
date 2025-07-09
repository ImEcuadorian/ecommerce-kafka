#!/bin/sh
set -e

# 1) Inicializa la base (crea inventory.db si no existe o la reinicia)
python src/app/init_db.py

# 2) Arranca el servicio
exec uvicorn src.app.main:app --host 0.0.0.0 --port 8001
