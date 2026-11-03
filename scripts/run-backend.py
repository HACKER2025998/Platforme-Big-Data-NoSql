#!/usr/bin/env python3
"""Démarre le JAR avec .env ; aucun secret ne passe dans les arguments."""
import os,subprocess
from pathlib import Path
from urllib.parse import quote
root=Path(__file__).resolve().parents[1]
env=os.environ.copy()
for line in (root/'.env').read_text().splitlines():
    if '=' in line and not line.startswith('#'):
        k,v=line.split('=',1);env[k]=v
user=quote(env.get('MONGO_APP_USERNAME','temperature_app'),safe='')
password=quote(env['MONGO_APP_PASSWORD'],safe='')
env.setdefault('MONGODB_URI',f'mongodb://{user}:{password}@localhost:27017/temperature?authSource=temperature&w=majority&journal=true&serverSelectionTimeoutMS=3000')
java=str(Path(env['JAVA_HOME'])/'bin/java') if env.get('JAVA_HOME') else 'java'
raise SystemExit(subprocess.call([java,'-jar',str(root/'backend/target/app.jar')],env=env))
