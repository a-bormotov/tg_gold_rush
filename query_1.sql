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
WHERE
  ur."resourceType" = 'gold'
  -- список userId из query_2.sql
  AND ur."userId" = ANY(%(uids)s::text[])          -- явный каст к text[]
  AND (
       ur."userId" LIKE 'line:' || '%'             -- без литерального %
    OR EXISTS (SELECT 1 FROM stars_transactions    st  WHERE st."userId" = ur."userId")
    OR EXISTS (SELECT 1 FROM stripe_transactions   stp WHERE stp."userId" = ur."userId")
    OR EXISTS (SELECT 1 FROM thirdweb_transactions tw  WHERE tw."userId" = ur."userId")
  )
  AND EXISTS (
    SELECT 1
    FROM users_challenges uc
    WHERE uc."userId" = ur."userId"
      AND substring(lower(uc."constellationType") from 'constellation([0-9]+)$')::int >= 10
  );
