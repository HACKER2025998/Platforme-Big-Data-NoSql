#!/usr/bin/env python3
"""Jeu de démonstration explicite, envoyé uniquement par l'API HTTP."""
import argparse,datetime,json,math,time,urllib.request,urllib.error,uuid
p=argparse.ArgumentParser();p.add_argument('--url',default='http://localhost:8080');a=p.parse_args()
def call(method,path,body=None):
 r=urllib.request.Request(a.url+'/api/v1'+path,data=json.dumps(body).encode() if body is not None else None,method=method,headers={'Content-Type':'application/json'})
 with urllib.request.urlopen(r,timeout=15) as f:return json.load(f)
now=datetime.datetime.now(datetime.timezone.utc)
for i,(name,threshold,base) in enumerate([('Salle serveurs',40,26),('Salle de réunion',35,23),('Laboratoire',40,25),('Salle de formation',35,22)],1):
 sensor=f'SALLE-{i:02}'
 try:call('POST','/capteurs',{'id':sensor,'libelle':f'Capteur principal {i}','salle':name,'seuilTemperature':threshold,'actif':True})
 except urllib.error.HTTPError as e:
  if e.code!=409:raise
  print(sensor,'existant : configuration conservée')
 for point in range(50):
  stamp=now-datetime.timedelta(seconds=(49-point)*60)
  temp=round(base+math.sin(point*.22)*2+math.cos(point*.83)*.45,1)
  if point==49 and i==1:temp=42.6
  call('POST','/mesures',{'eventId':str(uuid.uuid4()),'capteurId':sensor,'date':stamp.isoformat(timespec='milliseconds'),'temperature':temp})
print('200 mesures reçues. Attendre leur traitement avant la démonstration.')
