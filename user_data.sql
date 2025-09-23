WITH ids(id, ord) AS (
  VALUES %s
)
SELECT
  ids.id AS "userId",
  CASE
    WHEN u.username IS NULL OR u.username = '' OR u.username = 'Secret Dino'
      THEN ids.id
    ELSE u.username
  END AS "username",
  ids.ord
FROM ids
LEFT JOIN users u
  ON u.id::text = ids.id
 AND u."createdAt"::date <= DATE '2025-09-12'   -- фильтр по дате ТУТ, в ON
ORDER BY ids.ord;
