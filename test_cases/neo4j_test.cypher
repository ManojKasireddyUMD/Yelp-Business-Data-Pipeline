// 1. Test: No Duplicate Business IDs
MATCH (b:Business)
WITH b.id AS businessId, COUNT(*) AS count
WHERE count > 1
RETURN businessId, count;

// 2. Test: Every Review is linked to a User and a Business
MATCH (r:Review)
WHERE NOT (r)<-[:WROTE]-(:User) OR NOT (r)-[:REVIEWS]->(:Business)
RETURN r.id AS review_id LIMIT 10;

// 3. Test: No Orphan Nodes (nodes without relationships)
MATCH (n)
WHERE NOT (n)--()
RETURN DISTINCT labels(n) AS label, COUNT(*) AS orphanCount;

// 4. Test: Business must have a City and State
MATCH (b:Business)
WHERE NOT (b)-[:LOCATED_IN]->(:City)
RETURN b.id AS business_id_missing_city
LIMIT 10;

MATCH (c:City)
WHERE NOT (c)-[:IN_STATE]->(:State)
RETURN c.name AS city_missing_state
LIMIT 10;

// 5. Test: Business must have at least one Category and Feature
MATCH (b:Business)
WHERE NOT (b)-[:HAS_CATEGORY]->(:Category)
RETURN b.id AS business_missing_category
LIMIT 10;

MATCH (b:Business)
WHERE NOT (b)-[:OFFERS]->(:Feature)
RETURN b.id AS business_missing_feature
LIMIT 10;

// 6. Test: Friend counts match relationships
MATCH (u:User)
WITH u, SIZE((u)-[:FRIENDS_WITH]->()) AS actualFriendCount
WHERE u.friend_count <> actualFriendCount
RETURN u.id AS user_id, u.friend_count AS stated, actualFriendCount
LIMIT 10;

// 7. Test: Check for uniqueness constraints (manual inspection)
CALL db.constraints();

// 8. Test: Top 5 reviewers of a known business
MATCH (u:User)-[w:WROTE]->(r:Review)-[:REVIEWS]->(b:Business {name: "Royal House"})
RETURN u.name, r.stars, w.sentiment_label
ORDER BY r.stars DESC, u.name
LIMIT 5;
