#!/usr/bin/env python3
"""Générateur HTTP indépendant ; aucune connexion directe aux bases."""
import argparse,datetime,json,random,time,urllib.request,urllib.error,uuid
p=argparse.ArgumentParser();p.add_argument('--url',default='http://localhost:8080');p.add_argument('--seed',action='store_true');p.add_argument('--count',type=int,default=10);p.add_argument('--interval',type=float,default=1);p.add_argument('--high',action='store_true');p.add_argument('--once',action='store_true');a=p.parse_args()
def call(path,body=None):
    r=urllib.request.Request(a.url+'/api/v1'+path,data=json.dumps(body).encode() if body else None,headers={'Content-Type':'application/json'})
    with urllib.request.urlopen(r,timeout=15) as f:return json.load(f)
if a.count<1 or a.count>100 or a.interval<=0:p.error('count : 1 à 100 ; interval > 0')
if a.seed:
    for i in range(1,a.count+1):
        body={'id':f'SALLE-{i:02}','libelle':f'Capteur {i:02}','salle':f'Salle {i:02}','seuilTemperature':40,'actif':True}
        try:call('/capteurs',body)
        except urllib.error.HTTPError as e:
            if e.code!=409:raise
            print(f'{body["id"]} existe : configuration conservée')
i=0
while True:
    sensor=f'SALLE-{i%a.count+1:02}'
    event={'eventId':str(uuid.uuid4()),'capteurId':sensor,'date':datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='milliseconds'),'temperature':round(45+random.random()*3 if a.high else 22+random.random()*7,2)}
    # Le même identifiant est conservé pendant les nouvelles tentatives HTTP.
    for retry in range(5):
        try:
            receipt=call('/mesures',event);print(sensor,event['temperature'],receipt['id'],flush=True);break
        except (urllib.error.URLError,TimeoutError):
            if retry==4:raise
            time.sleep(min(8,2**retry))
    i+=1
    if a.once and i>=a.count:break
    time.sleep(a.interval)
