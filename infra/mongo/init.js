// Réexécutable avec le compte administrateur, sans suppression de données.
const app = db.getSiblingDB('temperature');
if (!app.getUser(process.env.MONGO_APP_USERNAME)) {
  app.createUser({user: process.env.MONGO_APP_USERNAME, pwd: process.env.MONGO_APP_PASSWORD,
    roles: [{role: 'readWrite', db: 'temperature'}]});
}
const schemas = {
  capteurs: {bsonType: 'object', required: ['_id','libelle','salle','seuilTemperature','actif','version'],
    properties: {_id: {bsonType: 'string', pattern: '^[A-Z0-9_-]{1,64}$'},
      libelle: {bsonType: 'string', minLength: 1, maxLength: 120},
      salle: {bsonType: 'string', minLength: 1, maxLength: 120},
      seuilTemperature: {bsonType: ['double','int','long','decimal'], minimum: -50, maximum: 100},
      actif: {bsonType: 'bool'}, version: {bsonType: ['int','long'], minimum: 0}}},
  ingestions: {bsonType: 'object', required: ['_id','event','empreinte','publicationState','processingState'],
    properties: {_id: {bsonType: 'string'}, event: {bsonType: 'object'},
      publicationState: {enum: ['PENDING','PUBLISHED']}, processingState: {enum: ['PENDING','PROCESSED','FAILED']}}},
  alertes: {bsonType: 'object', required: ['_id','capteurId','date','temperature','seuilApplique','statut'],
    properties: {_id: {bsonType: 'string'}, statut: {enum: ['OPEN','ACKNOWLEDGED']}}}
};
for (const [name, schema] of Object.entries(schemas)) {
  const validator = {$jsonSchema: schema};
  if (!app.getCollectionNames().includes(name)) app.createCollection(name, {validator});
  else { const result = app.runCommand({collMod: name, validator, validationLevel: 'strict'}); if (result.ok !== 1) throw new Error('Migration Mongo échouée : '+name); }
}
app.capteurs.createIndex({salle: 1, actif: 1, _id: 1});
app.ingestions.createIndex({publicationState: 1, nextAttemptAt: 1, _id: 1});
app.alertes.createIndex({capteurId: 1, statut: 1, date: -1, _id: -1});
app.alertes.createIndex({statut: 1, date: -1, _id: -1});
