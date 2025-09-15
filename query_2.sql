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
),
gold_cte AS (
  SELECT
    e."userId",
    SUM( COALESCE((e.payload::jsonb #>> '{output,gold,amount}')::bigint, 0) ) +
	SUM( COALESCE((e.payload::jsonb #>> '{output,rewards,gold,amount}')::bigint, 0) ) AS gold
  FROM events e
  WHERE
    e."createdAt" >= TIMESTAMP '2025-09-11 16:00:00'
    AND e."createdAt" <  TIMESTAMP '2025-09-16 16:00:00'
    AND e."name" IN ('ClaimChallengesAction','UnlockChallengeAction')
  GROUP BY e."userId"
)
SELECT
  f."userId",
  COUNT(*) FILTER (WHERE f.rarity = 0) AS rares,
  COUNT(*) FILTER (WHERE f.rarity = 1) AS epics,
  COUNT(*) FILTER (WHERE f.rarity = 2) AS legendaries,
  COALESCE(g.gold, 0)                    AS gold
FROM flattened f
LEFT JOIN gold_cte g ON g."userId" = f."userId"
GROUP BY f."userId", g.gold;
