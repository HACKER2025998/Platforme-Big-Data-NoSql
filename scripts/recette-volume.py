#!/usr/bin/env python3
"""Contrôle du budget statistique sur une pile locale de test uniquement."""
import argparse,csv,datetime,json,subprocess,tempfile,urllib.request,urllib.error,urllib.parse,uuid
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--test-stack',required=True,choices=['temperature']);a=p.parse_args()
root=Path(__file__).resolve().parents[1];sensor='VOLUME-'+uuid.uuid4().hex[:8].upper();now=datetime.datetime.now(datetime.timezone.utc);day=now.date();date=(now-datetime.timedelta(minutes=10)).strftime('%Y-%m-%d %H:%M:%S.%f+0000')
req=urllib.request.Request('http://localhost:8080/api/v1/capteurs',data=json.dumps({'id':sensor,'libelle':'Budget statistiques','salle':'Recette volume','seuilTemperature':40}).encode(),headers={'Content-Type':'application/json'},method='POST')
with urllib.request.urlopen(req,timeout=15) as r:assert r.status==201
with tempfile.TemporaryDirectory(prefix='temperature-volume-') as folder:
 file=Path(folder)/'volume.csv'
 with file.open('w',newline='') as f:
  writer=csv.writer(f);writer.writerow(['capteur_id','jour','date_mesure','event_id','temperature'])
  for i in range(10001):writer.writerow([sensor,day,date,str(uuid.UUID(int=i+1)),25])
 compose=[str(root/'scripts/compose.sh')]
 subprocess.run([*compose,'cp',str(file),'cassandra:/tmp/temperature-volume.csv'],check=True,capture_output=True)
 result=subprocess.run([*compose,'exec','-T','cassandra','sh','-ec',"cqlsh -u temperature_app -p \"$CASSANDRA_APP_PASSWORD\" -e \"COPY temperature.mesures_par_capteur_jour (capteur_id,jour,date_mesure,event_id,temperature) FROM '/tmp/temperature-volume.csv' WITH HEADER = TRUE\""],check=True,capture_output=True,text=True)
 assert "10001 rows imported" in result.stdout, "Import incomplet"
period=urllib.parse.urlencode({'du':(now-datetime.timedelta(hours=1)).isoformat(),'au':now.isoformat()})
try:urllib.request.urlopen('http://localhost:8080/api/v1/capteurs/'+sensor+'/statistiques?'+period,timeout=30)
except urllib.error.HTTPError as e:assert e.code==422,e.read();print('PASS 10001 mesures : HTTP 422, aucun agrégat tronqué')
else:raise AssertionError('Le dépassement de budget doit être refusé')
with urllib.request.urlopen('http://localhost:8080/api/v1/capteurs/'+sensor+'/mesures?'+period+'&limite=500',timeout=30) as r:
 page=json.load(r);assert len(page['items'])==500 and page['nextCursor'];print('PASS 10001 mesures : historique limité à 500 et curseur fourni')
