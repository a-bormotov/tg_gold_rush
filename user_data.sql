-- Ожидает, что в текущей сессии уже создана TEMP TABLE ids(id text, ord int).
-- Возвращает userId, username, ord в исходном порядке.

WITH src AS (
  SELECT id, ord FROM ids
)
SELECT
  src.id AS "userId",
  CASE
    WHEN u.username = 'Secret Dino' OR u.username IS NULL OR u.username = ''
      THEN src.id
    ELSE u.username
  END AS username,
  src.ord
FROM src
LEFT JOIN users u
  ON u.id::text = src.id
ORDER BY src.ord;
