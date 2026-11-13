-- =====================================================================
-- SEED: route_reference  (Lab 3, Step 8)
--
-- Five carriers. A bounded INSERT: it runs as a short Flink job and finishes. Running it
-- again is harmless — carrier_code is the primary key, so each row is an upsert.
--
-- Submit on its own with:  docker compose exec sql-client /opt/submit.sh seed
-- =====================================================================

SET 'pipeline.name' = 'route-reference-seed';

INSERT INTO fluss.app.route_reference VALUES
  ('DHL', 'DHL Express',          'Frankfurt',  2),
  ('UPS', 'UPS Worldwide',        'Cologne',    3),
  ('FDX', 'FedEx International',  'Amsterdam',  2),
  ('GLS', 'GLS Group',            'Bologna',    4),
  ('TNT', 'TNT Express',          'Liège',      3);
