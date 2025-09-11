WITH agg AS (
  SELECT ur."userId", SUM(ur.amount) AS gold
  FROM users_resources ur
  WHERE ur."resourceType" = 'gold'
  GROUP BY ur."userId"
)
SELECT 
  a."userId",
  CASE 
    WHEN u.username = 'Secret Dino' OR u.username IS NULL OR u.username = '' 
      THEN a."userId"::text
    ELSE u.username
  END AS username,
  a.gold
FROM agg a
LEFT JOIN users u ON u.id = a."userId"
ORDER BY a.gold DESC 
LIMIT 3000;

