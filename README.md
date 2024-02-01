# mongodb-filter-to-postgres

A simple package that converts a [Mongodb Query Filter](https://www.mongodb.com/docs/compass/current/query/filter)
to a Postgres WHERE clause. Aiming to be simple and safe.

**Project State:** Just a rough sketch and playground for Erik and Koen.

## Example:

Let's filter some lobbies in a multiplayer game:
```json5
{
  "$and": [
    {
      "$or": [                                // match two maps
        { "map": { "$contains": "aztec" } },
        { "map": { "$contains": "nuke" } }
      ]
    },
    { "password": "" },                       // no password set
    {
      "playerCount": { "$gte": 2, "$lt": 10 } // not empty or full
    }
  ]
}
```
Converts to:
```sql
(
  "customdata"->>"map" LIKE ?
  OR
  "customdata"->>"map" LIKE ?
) 
AND "password" = ? 
AND (
  "playerCount" >= ?
  AND
  "playerCount" < ?
)
```
And values:
```go
values := []any{"%aztec%", "%nuke%", "", 2, 10}
```
(given "map" is confugired to be in a jsonb column)
