-- Итог: userId, score, purple, legendaries
-- score = purple * (1 + legendaries * 0.10)

WITH ewin AS (
  SELECT *
  FROM events
  WHERE "createdAt" >= TIMESTAMP '2025-09-19 16:00:00'
    AND "createdAt" <  TIMESTAMP '2025-09-26 16:00:00'
    AND "name" IN ('ClaimChallengesAction','UnlockChallengeAction','SpendGachaAction')
),

purple AS (
  SELECT
    e."userId",
    SUM(
      COALESCE((e.payload::jsonb #>> '{output,purpleStones,amount}')::bigint, 0) +
      COALESCE((e.payload::jsonb #>> '{output,rewards,purpleStones,amount}')::bigint, 0)
    ) AS purple
  FROM ewin e
  WHERE e."name" IN ('ClaimChallengesAction','UnlockChallengeAction')
  GROUP BY e."userId"
),

legend AS (
  SELECT
    e."userId",
    COUNT(*) FILTER (WHERE (item->>'rarity')::int = 2) AS legendaries
  FROM ewin e
  CROSS JOIN LATERAL jsonb_array_elements(e.payload::jsonb->'output') AS item
  WHERE e."name" = 'SpendGachaAction'
  GROUP BY e."userId"
)

SELECT
  COALESCE(p."userId", l."userId")                                              AS "userId",
  (COALESCE(p.purple, 0)::numeric) * (1 + COALESCE(l.legendaries, 0) * 0.10)    AS score,
  COALESCE(p.purple, 0)                                                         AS purple,
  COALESCE(l.legendaries, 0)                                                    AS legendaries
FROM purple p
FULL OUTER JOIN legend l
  ON p."userId" = l."userId"
ORDER BY score DESC, purple DESC, legendaries DESC
