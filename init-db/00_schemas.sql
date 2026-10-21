-- Exécuté automatiquement par postgres-dwh au PREMIER démarrage (volume vide).
-- Les tables sont définies dans schema.sql (étape 4 du README).

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS analytics;
