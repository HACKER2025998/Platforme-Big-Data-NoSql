#!/usr/bin/env python3
"""Tests de panne réservés à la pile locale de recette ; redémarrage garanti."""
import argparse,subprocess,json,time,uuid,datetime,urllib.request,urllib.error
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--test-stack',required=True,choices=['temperature']);p.add_argument('--report',default='/tmp/temperature-pannes.json');a=p.parse_args()
root=Path(__file__).resolve().parents[1];results=[]
def compose(*args,input=None):return subprocess.run([str(root/'scripts/compose.sh'),*args],input=input,text=True,check=True,capture_output=True).stdout
def call(method,path,body=None,expected=200):
 r=urllib.request.Request('http://localhost:8080'+path,data=json.dumps(body).encode() if body is not None else None,method=method,headers={'Content-Type':'application/json'})
 try:
  with urllib.request.urlopen(r,timeout=20) as f:status=f.status;data=f.read()
 except urllib.error.HTTPError as e:status=e.code;data=e.read()
 assert status==expected,(path,status,data[:150])
 return json.loads(data) if data else None
def test(name,ok=True):
 assert ok,name
 results.append(name);print('PASS '+name,flush=True)
def wait(fn,timeout=100):
 end=time.monotonic()+timeout
 while time.monotonic()<end:
  try:
   if fn():return
  except Exception:pass
  time.sleep(1)
 raise AssertionError('Délai dépassé')
sensor='PANNE-'+uuid.uuid4().hex[:8].upper();base='/api/v1'
call('POST',base+'/capteurs',{'id':sensor,'libelle':'Pannes','salle':'Recette pannes','seuilTemperature':40,'actif':True},201)
def measure(temp):return {'eventId':str(uuid.uuid4()),'capteurId':sensor,'date':datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='milliseconds'),'temperature':temp}
def processed(e):return call('GET',base+'/ingestions/'+e['eventId'])['processingState']=='PROCESSED'
stopped=[]
finished=False
try:
 e=measure(23);call('POST',base+'/mesures',e,202);wait(lambda:processed(e))
 compose('stop','redis');stopped.append('redis')
 test('Repli Cassandra pendant panne Redis',call('GET',base+'/capteurs/'+sensor+'/derniere-mesure')['eventId']==e['eventId'])
 e2=measure(44);call('POST',base+'/mesures',e2,202);wait(lambda:processed(e2));test('Traitement durable sans Redis')
 test('Readiness reste UP sans Redis',call('GET','/actuator/health/readiness')['status']=='UP')
 compose('start','redis');stopped.remove('redis')
 compose('stop','kafka');stopped.append('kafka')
 e3=measure(24);r=call('POST',base+'/mesures',e3,202);test('Kafka indisponible : 202 durable',r['publicationState']=='PENDING')
 test('Liveness indépendante de Kafka',call('GET','/actuator/health/liveness')['status']=='UP')
 compose('restart','backend')
 wait(lambda:call('GET','/actuator/health/liveness')['status']=='UP')
 test('Outbox conservée après redémarrage Java',call('GET',base+'/ingestions/'+e3['eventId'])['publicationState']=='PENDING')
 call('GET','/actuator/health/readiness',expected=503);test('Readiness détecte la panne Kafka')
 compose('start','kafka');stopped.remove('kafka');wait(lambda:processed(e3),150);test('Outbox reprise après rétablissement Kafka')
 compose('stop','mongodb');stopped.append('mongodb')
 call('POST',base+'/mesures',measure(22),503);test('Mongo indisponible : aucun faux HTTP 202')
 compose('start','mongodb');stopped.remove('mongodb');wait(lambda:call('GET','/actuator/health/readiness')['status']=='UP')
 test('Reçu conservé après redémarrage Mongo',processed(e3))
 # Rejouer directement le snapshot par Kafka après acquittement, avec la vraie clé.
 ack=call('PATCH',base+'/alertes/'+e2['eventId']+'/acquittement');receipt=call('GET',base+'/ingestions/'+e2['eventId'])
 raw=sensor+'|'+json.dumps(receipt['event'],separators=(',',':'))+'\n'
 compose('exec','-T','kafka','/opt/kafka/bin/kafka-console-producer.sh','--bootstrap-server','kafka:29092','--topic','mesures-temperature','--property','parse.key=true','--property','key.separator=|',input=raw)
 time.sleep(3)
 again=call('PATCH',base+'/alertes/'+e2['eventId']+'/acquittement');test('Rejeu Kafka préserve acquittement',ack['dateAcquittement']==again['dateAcquittement'])
 alerts=call('GET',base+'/alertes?capteurId='+sensor)['items'];test('Rejeu Kafka : une seule alerte',len(alerts)==1)
 # Message illisible : doit être envoyé vers DLQ et le consommateur doit continuer.
 compose('exec','-T','kafka','/opt/kafka/bin/kafka-console-producer.sh','--bootstrap-server','kafka:29092','--topic','mesures-temperature',input='invalid-json-recette\n')
 dlq=compose('exec','-T','kafka','/opt/kafka/bin/kafka-console-consumer.sh','--bootstrap-server','kafka:29092','--topic','mesures-temperature-dlq','--from-beginning','--timeout-ms','20000','--max-messages','1')
 test('JSON illisible conservé dans DLQ','invalid-json-recette' in dlq)
 e4=measure(25);call('POST',base+'/mesures',e4,202);wait(lambda:processed(e4));test('Consommation continue après DLQ')
 finished=True
finally:
 for service in stopped:compose('start',service)
 Path(a.report).write_text(json.dumps({'date':datetime.datetime.now(datetime.timezone.utc).isoformat(),'checks':results,'passed':len(results),'failed':0 if finished else 1},indent=2,ensure_ascii=False)+'\n')
