WITH ids AS (
  SELECT id, ord
  FROM UNNEST(%s::bigint[]) WITH ORDINALITY AS t(id, ord)
)
SELECT
  i.id AS "userId",
  CASE
    WHEN u.username = 'Secret Dino' OR u.username IS NULL OR u.username = ''
      THEN i.id::text
    ELSE u.username
  END AS username,
  i.ord
FROM ids i
LEFT JOIN users u ON u.id = i.id
ORDER BY i.ord
