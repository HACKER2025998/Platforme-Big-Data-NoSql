CREATE KEYSPACE IF NOT EXISTS temperature WITH replication = {'class':'NetworkTopologyStrategy','datacenter1':1};
CREATE TABLE IF NOT EXISTS temperature.mesures_par_capteur_jour (capteur_id text, jour date, date_mesure timestamp, event_id text, temperature decimal, PRIMARY KEY ((capteur_id,jour),date_mesure,event_id)) WITH CLUSTERING ORDER BY (date_mesure DESC,event_id DESC);
CREATE TABLE IF NOT EXISTS temperature.derniere_mesure (capteur_id text PRIMARY KEY, sort_key text, date_mesure timestamp, event_id text, temperature decimal);
