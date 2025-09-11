WITH agg AS (
  SELECT ur."userId", SUM(ur.amount) AS gold
  FROM users_resources_total ur
  WHERE ur."resourceType" = 'gold'
  GROUP BY ur."userId"
)
SELECT 
  CASE 
    WHEN u.username = 'Secret Dino' OR u.username IS NULL OR u.username = '' 
      THEN a."userId"::text
    ELSE u.username
  END AS username,
  a.gold,
  a."userId"
FROM agg a
LEFT JOIN users u ON u.id = a."userId"
order by a.gold desc 
limit 1000