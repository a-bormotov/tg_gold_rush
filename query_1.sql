SELECT 
  CASE 
    WHEN u.username = 'Secret Dino' OR u.username IS NULL OR u.username = '' 
      THEN ur."userId"::text
    ELSE u.username
  END AS username,
  ur.amount AS gold,
  ur."userId"
FROM users_resources_total ur
LEFT JOIN users u ON u.id = ur."userId"
WHERE ur."resourceType" = 'gold'
ORDER BY ur.amount DESC 
LIMIT 1000