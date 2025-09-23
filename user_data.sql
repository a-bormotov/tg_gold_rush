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
JOIN users u
  ON u.id::text = ids.id
WHERE
  u."createdAt"::date <= DATE '2025-09-12'   -- поставь нужную дату (включительно)
  AND ids.id NOT ILIKE 'line%'               -- исключаем ID, начинающиеся на "line"
ORDER BY ids.ord;
