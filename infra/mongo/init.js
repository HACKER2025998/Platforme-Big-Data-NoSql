const app = db.getSiblingDB('temperature');
if (!app.getUser(process.env.MONGO_APP_USERNAME)) app.createUser({user:process.env.MONGO_APP_USERNAME,pwd:process.env.MONGO_APP_PASSWORD,roles:[{role:'readWrite',db:'temperature'}]});
app.capteurs.createIndex({salle:1,actif:1,_id:1});
app.ingestions.createIndex({publicationState:1,nextAttemptAt:1,_id:1});
app.alertes.createIndex({capteurId:1,statut:1,date:-1,_id:-1});
app.alertes.createIndex({statut:1,date:-1,_id:-1});
