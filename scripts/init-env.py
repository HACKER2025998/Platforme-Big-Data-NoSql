#!/usr/bin/env python3
"""Crée une configuration privée sans écraser les secrets existants."""
from pathlib import Path
import secrets
p=Path('.env')
values={}
if p.exists():
    for line in p.read_text().splitlines():
        if '=' in line and not line.startswith('#'):
            k,v=line.split('=',1); values[k]=v
for k in ['MONGO_ROOT_PASSWORD','MONGO_APP_PASSWORD','REDIS_PASSWORD','CURSOR_SECRET','CASSANDRA_APP_PASSWORD','CASSANDRA_ADMIN_PASSWORD']:
    if not values.get(k): values[k]=secrets.token_hex(24)
values.setdefault('MONGO_ROOT_USERNAME','admin')
values.setdefault('MONGO_APP_USERNAME','temperature_app')
p.write_text('\n'.join(k+'='+v for k,v in values.items())+'\n')
p.chmod(0o600)
print('Configuration privée prête : .env (permissions 600).')
