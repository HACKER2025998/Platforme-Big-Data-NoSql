#!/usr/bin/env python3
"""Recette HTTP réelle ; identifiants dédiés, attentes bornées, échec non masqué."""
import argparse,datetime,json,time,urllib.request,urllib.error,urllib.parse,uuid,sys
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--url',default='http://localhost:8080');p.add_argument('--report',default='/tmp/temperature-recette.json');a=p.parse_args()
checks=[]
def check(name,ok):
    checks.append({'name':name,'passed':bool(ok)})
    print(('PASS ' if ok else 'FAIL ')+name,flush=True)
    if not ok:raise AssertionError(name)
def call(method,path,body=None,expected=200):
    req=urllib.request.Request(a.url+path,data=json.dumps(body).encode() if body is not None else None,method=method,headers={'Content-Type':'application/json'})
    try:
        with urllib.request.urlopen(req,timeout=20) as r:status=r.status;payload=r.read();headers=r.headers
    except urllib.error.HTTPError as e:status=e.code;payload=e.read();headers=e.headers
    if status!=expected:raise AssertionError(f'{method} {path}: {status} != {expected}; {payload[:300]!r}')
    return json.loads(payload) if payload else None
base='/api/v1';sensor='TEST-'+uuid.uuid4().hex[:12].upper()
now=datetime.datetime.now(datetime.timezone.utc);stamp=lambda x:x.isoformat(timespec='milliseconds')
def event(temp,seconds=0):return {'eventId':str(uuid.uuid4()),'capteurId':sensor,'date':stamp(now+datetime.timedelta(seconds=seconds)),'temperature':temp}
def processed(e):
    deadline=time.monotonic()+90
    while time.monotonic()<deadline:
        r=call('GET',base+'/ingestions/'+e['eventId'])
        if r['processingState']=='PROCESSED':return r
        if r['processingState']=='FAILED':raise AssertionError('Ingestion FAILED: '+str(r))
        time.sleep(.5)
    raise AssertionError('Traitement non terminé après 90 secondes')
try:
    check('Liveness',call('GET','/actuator/health/liveness')['status']=='UP')
    check('Readiness',call('GET','/actuator/health/readiness')['status']=='UP')
    data={'id':sensor,'libelle':'Recette HTTP','salle':'Salle recette','seuilTemperature':40,'actif':True}
    s=call('POST',base+'/capteurs',data,201);check('Création capteur',s['id']==sensor and s['version']==0)
    call('POST',base+'/capteurs',data,409);check('Doublon capteur refusé',True)
    call('GET',base+'/capteurs/'+sensor+'/derniere-mesure',expected=204);check('Aucune mesure : HTTP 204',True)
    check('Lecture fiche',call('GET',base+'/capteurs/'+sensor)['salle']=='Salle recette')
    check('Liste filtrée',any(x['id']==sensor for x in call('GET',base+'/capteurs?salle=Salle%20recette&taille=100')['items']))
    s=call('PATCH',base+'/capteurs/'+sensor,{'version':0,'libelle':'Modifié'});check('Modification versionnée',s['version']==1)
    call('PATCH',base+'/capteurs/'+sensor,{'version':0,'actif':False},409);check('Conflit de version',True)
    for body in [{'version':1},{'version':1,'salle':None},{'version':1,'inconnu':1}]:call('PATCH',base+'/capteurs/'+sensor,body,400)
    check('PATCH vide, null et champ inconnu refusés',True)
    normal=event(25,-10);high=event(45.2);old=event(20,-86400);equal=event(40,-20);same=event(26,-10)
    for e in [normal,high,old,equal,same]:
        r=call('POST',base+'/mesures',e,202);check('202 durable '+str(e['temperature']),r['id']==e['eventId']);processed(e)
    check('Dernière mesure monotone',call('GET',base+'/capteurs/'+sensor+'/derniere-mesure')['eventId']==high['eventId'])
    al=call('GET',base+'/alertes?capteurId='+sensor)['items'];check('Une alerte au-dessus du seuil strict',len(al)==1 and al[0]['seuilApplique']==40)
    ack=call('PATCH',base+'/alertes/'+high['eventId']+'/acquittement');ack2=call('PATCH',base+'/alertes/'+high['eventId']+'/acquittement');check('Acquittement idempotent',ack['dateAcquittement']==ack2['dateAcquittement'])
    call('PATCH',base+'/capteurs/'+sensor,{'version':1,'seuilTemperature':10,'actif':False})
    dup=call('POST',base+'/mesures',high,202);check('Rejeu conserve le seuil et fonctionne après désactivation',dup['event']['seuilApplique']==40)
    call('POST',base+'/mesures',{**high,'temperature':46},409);check('Rejeu contradictoire refusé',True)
    call('POST',base+'/mesures',event(23,1),409);check('Nouvelle mesure sur capteur inactif refusée',True)
    call('PATCH',base+'/capteurs/'+sensor,{'version':2,'actif':True,'seuilTemperature':40})
    period=urllib.parse.urlencode({'du':stamp(now-datetime.timedelta(days=2)),'au':stamp(now+datetime.timedelta(minutes=1))})
    path=base+'/capteurs/'+sensor+'/mesures?'+period+'&limite=2';h=call('GET',path);items=h['items'];cursor=h['nextCursor']
    while cursor:
        page=call('GET',path+'&curseur='+urllib.parse.quote(cursor));items+=page['items'];cursor=page['nextCursor']
    check('Pagination multijour sans doublon ni perte',len(items)==5 and len({x['eventId'] for x in items})==5)
    check('Tri stable aux dates égales',[(x['date'],x['eventId']) for x in items]==sorted([(x['date'],x['eventId']) for x in items],reverse=True))
    call('GET',path+'&curseur=forged',expected=400);check('Curseur falsifié refusé',True)
    stat=call('GET',base+'/capteurs/'+sensor+'/statistiques?'+period);check('Statistiques exactes',stat['count']==5 and stat['min']==20 and stat['max']==45.2 and abs(stat['moyenne']-31.24)<.0001)
    for body in [{**event(101)},{**event(23),'date':'1999-01-01T00:00:00Z'},{**event(23),'date':stamp(now+datetime.timedelta(hours=1))},{**event(23),'eventId':'x'},{**event(23),'extra':1}]:call('POST',base+'/mesures',body,400)
    check('Bornes physiques, dates, UUID et champs inconnus',True)
    call('POST',base+'/mesures',{**event(23),'capteurId':'INCONNU-RECETTE'},404);check('Capteur inconnu refusé',True)
    call('GET',base+'/capteurs?taille=101',expected=400);call('GET',base+'/alertes?statut=INVALID',expected=400);check('Pagination et filtres invalides refusés',True)
    invalid=urllib.parse.urlencode({'du':stamp(now),'au':stamp(now+datetime.timedelta(days=8))});call('GET',base+'/capteurs/'+sensor+'/mesures?'+invalid,expected=400);check('Période de plus de sept jours refusée',True)
finally:
    report={'date':datetime.datetime.now(datetime.timezone.utc).isoformat(),'url':a.url,'checks':checks,'passed':sum(x['passed'] for x in checks),'failed':sum(not x['passed'] for x in checks)}
    Path(a.report).parent.mkdir(parents=True,exist_ok=True);Path(a.report).write_text(json.dumps(report,indent=2,ensure_ascii=False)+'\n')
