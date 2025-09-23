WITH ids(id, ord) AS (
  VALUES %s
)
SELECT
  ids.id AS "userId",
  CASE
    WHEN u.username = 'Secret Dino' OR u.username IS NULL OR u.username = ''
      THEN ids.id
    ELSE u.username
  END AS "username",
  ids.ord
FROM ids
LEFT JOIN users u
  ON u.id::text = ids.id
WHERE
  u."createdAt" IS NULL
  OR u."createdAt"::date <= DATE '2025-09-12'  -- <= поставь нужную дату
ORDER BY ids.ord;
