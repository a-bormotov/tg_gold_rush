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
-- здесь реально фильтруем: оставляем либо тех, у кого нет строки в users,
-- либо у кого createdAt не позже отсечки
WHERE
  u.id IS NULL
  OR u."createdAt"::date <= DATE '2025-09-12'   -- поставь нужную дату
ORDER BY ids.ord;
