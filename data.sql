WITH ewin AS (
  SELECT *
  FROM events
  WHERE "createdAt" >= TIMESTAMPTZ '2025-09-23 16:00:00+00'
	AND "createdAt" <  TIMESTAMPTZ '2025-09-29 16:00:00+00'
    AND "name" IN ('ClaimChallengesAction','UnlockChallengeAction','SpendGachaAction')
),
purple AS (
  SELECT
    e."userId",
    SUM(
      COALESCE(NULLIF(e.payload::jsonb #>> '{output,purpleStones,amount}','')::bigint, 0) +
      COALESCE(NULLIF(e.payload::jsonb #>> '{output,rewards,purpleStones,amount}','')::bigint, 0)
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
  CROSS JOIN LATERAL jsonb_array_elements(
    CASE
      WHEN jsonb_typeof(e.payload::jsonb->'output') = 'array'
        THEN e.payload::jsonb->'output'
      ELSE '[]'::jsonb
    END
  ) AS item
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
ORDER BY score DESC, purple DESC, legendaries DESC;
