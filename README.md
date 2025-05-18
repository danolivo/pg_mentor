# pg_mentor

Employ query statistics stored in pg_stat_statements to decide which type of plan mode (custom, generic or auto) to use.

# How to use
Install it, load on startup as well as the `pg_stat_statements` module and call:
`CREATE EXTENSION pg_mentor`. Use the `CASCADE` word or create the `pg_stat_statements` manually in advance.

# Desired pg_stat_statements settings

Considering it manages prepared statements, it would be profitable to tune the extension a little bit with settings:

```
pg_stat_statements.track_utility = 'off'
pg_stat_statements.track = 'all'
pg_stat_statements.track_planning = 'on'
```
