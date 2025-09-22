-- Принимает один параметр: %s :: text[]
-- Сохраняем порядок через WITH ORDINALITY
WITH ids AS (
  SELECT id, ord
  FROM UNNEST(%s::text[]) WITH ORDINALITY AS t(id, ord)
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
LEFT JOIN users u ON u.id = i.id  -- обе стороны text, без кастов
ORDER BY i.ord;
