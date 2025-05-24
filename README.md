# pg_mentor

Employ query statistics stored in the `pg_stat_statements` extension to decide which type of plan mode (custom, generic or auto) to use.

The extension maintains a shared hash table with query IDs and plan cache modes. To analyse the `pg_stat_statements` and add some data to this table, you may call specific routines and choose one of the following strategies:

- `pg_mentor_nail_long_planned` - forces a generic plan for queries for which the max execution time is less than the average planning time.

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

# Testing
- Quick-check on dummy issues - see this [wiki page](https://github.com/danolivo/pg_mentor/wiki/How-to-pass-make-check).
