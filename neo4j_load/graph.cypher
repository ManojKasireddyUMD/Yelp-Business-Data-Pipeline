// -----------------------------
// Yelp Graph Schema for Neo4j
// -----------------------------
// Purpose: Build a fully connected, production-grade Neo4j graph from Yelp datasets
// Datasets: business.csv, user.csv, review.csv, business_geo.csv, business_category.csv, business_feature.csv, friendships.csv

// 0. CLEANING: Start with a clean slate by deleting all existing nodes and relationships
MATCH (n) DETACH DELETE n;

// 1. SCHEMA CONSTRAINTS: Uniqueness on IDs for major node labels
CREATE CONSTRAINT business_id_unique IF NOT EXISTS FOR (b:Business) REQUIRE b.id IS UNIQUE;
CREATE CONSTRAINT user_id_unique IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE;
CREATE CONSTRAINT review_id_unique IF NOT EXISTS FOR (r:Review) REQUIRE r.id IS UNIQUE;

// 2. LOOKUP NODE CONSTRAINTS: Unique names for helper lookup entities
CREATE CONSTRAINT city_name_unique FOR (c:City) REQUIRE c.name IS UNIQUE;
CREATE CONSTRAINT state_name_unique FOR (s:State) REQUIRE s.name IS UNIQUE;
CREATE CONSTRAINT category_name_unique FOR (cat:Category) REQUIRE cat.name IS UNIQUE;
CREATE CONSTRAINT feature_name_unique FOR (f:Feature) REQUIRE f.name IS UNIQUE;

// 3. BUSINESS NODES: Load and create businesses with properties
CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'https://…/business.csv' AS row",
  "MERGE (b:Business {id: row.id})
   SET b.name = row.name,
       b.latitude = toFloat(row.latitude),
       b.longitude = toFloat(row.longitude),
       b.average_stars = toFloat(row.stars),
       b.review_count = toInteger(row.review_count),
       b.accepts_credit_cards = row.accepts_credit_cards,
       b.noise_level = row.noise_level",
  {batchSize:1000, parallel:true}
);

// 4. USER NODES: Create users with attributes related to social and review metrics
CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'https://…/user.csv' AS row",
  "MERGE (u:User {id: row.id})
   SET u.name = row.name,
       u.fans = toInteger(row.fans),
       u.elite_years_count = toInteger(row.elite_years_count),
       u.friend_count = toInteger(row.friends),
       u.engagement_score = toFloat(row.engagement_compliments)",
  {batchSize:1000, parallel:true}
);

// 5. REVIEW NODES: Link users to reviews and reviews to businesses
CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'https://…/review.csv' AS row",
  "MATCH (u:User {id: row.user_id}),
         (b:Business {id: row.business_id})
   MERGE (r:Review {id: row.id})
   SET r.stars = toFloat(row.stars),
       r.date = date(row.date),
       r.sentiment_label = row.sentiment_label
   MERGE (u)-[:WROTE {date: r.date, sentiment_label: r.sentiment_label}]->(r)
   MERGE (r)-[:REVIEWS]->(b)",
  {batchSize:1000, parallel:true}
);

// 6. GEOGRAPHY: City-State hierarchy and linking business to city
CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'https://…/business_geo.csv' AS row",
  "MERGE (c:City {name: row.city})
   MERGE (s:State {name: row.state})
   MERGE (c)-[:IN_STATE]->(s)
   MATCH (b:Business {id: row.business_id})
   MERGE (b)-[:LOCATED_IN]->(c)",
  {batchSize:1000, parallel:true}
);

// 7. CATEGORIES: Businesses and their category affiliations
CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'https://…/business_category.csv' AS row",
  "MATCH (b:Business {id: row.business_id})
   MERGE (cat:Category {name: row.category})
   MERGE (b)-[:HAS_CATEGORY]->(cat)",
  {batchSize:1000, parallel:true}
);

// 8. FEATURES: Services or amenities offered by businesses
CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'https://…/business_feature.csv' AS row",
  "MATCH (b:Business {id: row.business_id})
   MERGE (f:Feature {name: row.feature})
   MERGE (b)-[:OFFERS]->(f)",
  {batchSize:1000, parallel:true}
);

// 9. FRIENDSHIPS: Bidirectional social connections between users
CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'https://…/friendships.csv' AS row",
  "MATCH (u1:User {id: row.user_id}), (u2:User {id: row.friend_id})
   MERGE (u1)-[:FRIENDS_WITH]->(u2)
   MERGE (u2)-[:FRIENDS_WITH]->(u1)",
  {batchSize:1000, parallel:true}
);

// -------------------------------
// Notes:
// - Each MERGE ensures nodes/relationships aren't duplicated.
// - Using APOC's batch loading prevents memory overflow for large files.
// - Relationships are meaningful and mirror real-world associations.
// - Constraints protect graph integrity and ensure performance.
// -------------------------------
