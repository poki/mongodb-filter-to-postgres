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
        { "map": { "$regex": "aztec" } },
        { "map": { "$regex": "nuke" } }
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
  "customdata"->>"map" ~* $1
  OR
  "customdata"->>"map" ~* $2
)
AND "password" = $3
AND (
  "playerCount" >= $4
  AND
  "playerCount" < $5
)
```
And values:
```go
values := []any{"%aztec%", "%nuke%", "", 2, 10}
```
(given "map" is confugired to be in a jsonb column)
