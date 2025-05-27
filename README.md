# pg_mentor

Lightweight extension that employs query statistics stored in the `pg_stat_statements` extension to decide which type of plan mode (custom, generic or auto) to use.

The extension maintains a shared hash table with query IDs and plan cache modes. To analyse the `pg_stat_statements` and add some data to this table you may call specific routines and choose one of the following strategies:

- `pg_mentor_nail_long_planned` - forces a generic plan for queries for which the max execution time is less than the average planning time.
- `reconsider_ps_modes` - passes through the statistics and decides how to switch (see section 'Plain Switch Strategy' for details).
- Use the `pg_mentor_set_plan_mode` function to force plan cache mode globally for specific queryId in manual mode.

# How to use
Install it, load on startup (or dynamically) into the database with the `pg_stat_statements` module installed and call:
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

# Additional functions
- `pg_mentor_show_prepared_statements` - shows the state of decision machine.
- `pg_mentor_reload_conf` - causes refresh of local plan parameters according to the global state. Usually isn't needed, just in case.

# Plain Switch Strategy

## User Interface
```
SELECT reconsider_ps_modes();
```
Should return single record with statistics on `to generic`, `to custom`, `not changed` plan mode decisions.

## Preliminaries
- Assume that `Average Execution Time` is too blurry (may depend on the `shared_buffers` state) and hardly floating because of averaging even when we reset statistics from time to time.
- We need MIN/MAX execution time to detect 'unstable' query. It may work if we reset the `pg_stat_statements` statistics from time to time.
- Assume, that basically, the optimiser have less statistic planning generic plan than the custom one. So, we shouldn't anticipate that generic plan improves query execution time (only occasionally). It reduces planning expenses. So, we should be OK with generic plan mode all the time when planning time dominates max execution time.

## Definitions

- **RT stamp** - reference execution time, have written into the `pg_mentor` before the plan mode switch.

## Algorithm
On arbitrary user call, analyse `pg_stat_statements`'s entries, related to any statements, prepared  over current (for the backend where user calls our function) database (joining by the queryId value) and:

I **At first:** (_probe looks-good-to-be-generic_)

1. Select candidate  plan with NULL RT `stamp`.
2. Check: `(max-min)/mean_exec_time` <= 2.0 OR (`total_exec_time <= total_plan_time`).
3. Switch it to the **generic** one
4. Save the `total_exec_time` value from the `pg_stat_statements` as  the`RT stamp`.
**NOTE**: number of executions should be more than one.

II **Second:** (_detect unsuccessful `to generic` switches_)

1. Select generic plan with non-null `RT stamp`.
2. Check: `total_exec_time / RT stamp > 2.0`
3. If execution time grows a lot, switch to the **custom** plan.
4. Save the `total_exec_time` value as the `RT stamp`.
**NOTE**: To be stable, such statement should be marked as 'fixed' to disallow reverting decision to be made - for the stability reason.

III **Third:** (_probe looks-good-to-be-custom_)

1. Select generic plan with NULL `RT stamp` (managed by the core).
2. Check: if `(max - min) /mean_exec_time` > 2 AND `total_exec_time > total_plan_time`
3. If query execution time is both _unstable_ and its execution _longer_ than planning, switch to the **custom** plan mode
4. Save the  `total_exec_time ` value as the `RT stamp`.
**NOTE**: in principle, we would OR the main filters, but it would intersect with `I` and need more details. Stay simpler for now.
**NOTE**: number of executions should be more than one.
**EXAMPLE**: query each time touches different number of partitions/tuples and scans lots of data to compensate planning.

IV **Fourth:** (_detect unsuccessful `to custom` switches_)

1. Select custom plan with non-null RT.
2. Check `RT stamp / total_exec_time < 2.0.
3. If the custom plan doesn't _speed up_ query execution enough or it has been planned too much time, switch to the generic plan mode.
4.  Save the  `total_exec_time` value as the `RT stamp`.

**Fifth:**

1. Reset `pg_stat_statements

## NOTES

- How to avoid fluctuations in plan mode switching? - we may introduce the `switch counter` that will limit number of switches, at least for the specific time range.
- Also, it would be better to remember previous state - at least, to not return to the bad state, 4 -> 2, as an example.
- Strategy doesn't cover all possible query states. Assume, that the query plan stays optimal after previous forcing attempt or just OK to be in AUTO mode, controlled by the core logic. We should force only corner cases, obviously bad for the performance.

NB! total_exec_time includes total_plan_time. So, we need to use some sort of `TET > TPT * 2.0`.

Attention!: EXPLAIN ONLY causes zero min_exec_time.
