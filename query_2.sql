WITH flattened AS (
  SELECT
    e."userId",
    (item->>'rarity')::int AS rarity
  FROM events e
  CROSS JOIN LATERAL jsonb_array_elements(e.payload::jsonb->'output') AS item
  WHERE
    e."createdAt" >= TIMESTAMP '2025-09-11 16:00:00'
    AND e."createdAt" <  TIMESTAMP '2025-09-16 16:00:00'
    AND e."name" = 'SpendGachaAction'
    AND (item->>'rarity') ~ '^[0-9]+$'
)
SELECT
  "userId",
  COUNT(*) FILTER (WHERE rarity = 0) AS rares,
  COUNT(*) FILTER (WHERE rarity = 1) AS epics,
  COUNT(*) FILTER (WHERE rarity = 2) AS legendaries
FROM flattened
GROUP BY "userId";
