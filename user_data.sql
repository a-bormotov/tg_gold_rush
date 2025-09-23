WITH ids(id, ord) AS (
  VALUES %s
)
SELECT
  ids.id AS "userId",
  CASE
    WHEN u.username = 'Secret Dino' OR u.username = ''
      THEN ids.id
    ELSE u.username
  END AS "username",
  ids.ord
FROM ids
JOIN users u
  ON u.id::text = ids.id
 AND u."createdAt"::date <= DATE '2025-09-12'
ORDER BY ids.ord;
