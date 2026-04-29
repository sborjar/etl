#!/bin/bash

# Cambiar al directorio del proyecto
cd /home/ec2-user/etl || exit

# Activar el entorno virtual
source ./.venv/bin/activate

# Ejecutar el script con la fecha de hoy
python main.py d $(date +\%Y-\%m-\%d)