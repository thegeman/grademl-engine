# GradeML Query Language

Goal: answer arbitrary questions about performance data. For example:

1. What is the average utilization of resource X?
2. What is the average utilization of resource X by phase Y?
3. What fraction of time is resource X used more than Y%?

## Example Queries

1. What is the average utilization of resource X?

```
FROM metrics
WHERE path ~= "X"
GROUP BY path
SELECT path,
  AVG_OVER_TIME(utilization, duration) AS avg_utilization;
```

2. What is the average utilization of resource X by phase Y?

```
FROM attributed_metrics
WHERE metric_path ~= "X" AND
  phase_path ~= "Y"
GROUP BY metric_path, phase_path
SELECT metric_path,
  phase_path,
  AVG_OVER_TIME(utilization, duration) AS avg_utilization;
```

3. What fraction of time is resource X used more than Y%?

```
FROM metrics AS M
WHERE M.path ~= "X"
GROUP BY M.path
SELECT M.path,
  AVG_OVER_TIME(M.utilization >= Y/100, M.duration) AS time_utilized;
```

## Data Model

Virtual tables for phase and metric data:

1. `phases` table:
    - `start_time`: seconds
    - `end_time`: seconds
    - `duration`: seconds
    - `path`: phase_path
    - `type`: phase_path
2. `metrics` table:
    - `start_time`: seconds
    - `end_time`: seconds
    - `duration`: seconds
    - `utilization`: fraction
    - `usage`: double
    - `capacity`: double
    - `path`: metric_path
    - `type`: metric_path
3. `attributed_metrics` table:
    - `start_time`: seconds
    - `end_time`: seconds
    - `duration`: seconds
    - `utilization`: fraction
    - `usage`: double
    - `capacity`: double
    - `metric_path`: metric_path
    - `metric_type`: metric_path
    - `phase_path`: phase_path
    - `phase_type`: phase_path

## Language specification

```
QUERY
  : FROM_CLAUSE WHERE_CLAUSE? GROUP_BY_CLAUSE? SELECT_CLAUSE FIRST_CLAUSE? LAST_CLAUSE?';'

GROUP_BY_CLAUSE
  : 'GROUP' 'BY' COLUMN_LITERAL (',' COLUMN_LITERAL)*

FROM_CLAUSE
  : 'FROM' TABLE_EXPR

TABLE_EXPR
  : ID ('AS' ID)?

WHERE_CLAUSE
  : 'WHERE' EXPR

SELECT_CLAUSE
  : 'SELECT' COLUMN_EXPR (',' COLUMN_EXPR)*

COLUMN_EXPR
  : EXPR ('AS' ID)?

LIMIT_CLAUSE
  : 'LIMIT' INTEGER
  | 'LIMIT' 'FIRST' INTEGER ('LAST' INTEGER)?
  | 'LIMIT' 'LAST' INTEGER

EXPR
  : EXPR 'OR' EXPR
  | EXPR 'AND' EXPR
  | EXPR ('==' | '=' | '!=' | '~=' | '!~') EXPR
  | EXPR ('+' | '-') EXPR
  | EXPR ('*' | '/') EXPR
  | 'NOT' EXPR
  | ID '(' ARG_LIST? ')'
  | '(' EXPR ')'
  | LITERAL

ARG_LIST
  : EXPR (',' EXPR)*

LITERAL
  : 'TRUE'
  | 'FALSE'
  | PATH_LITERAL
  | NUMERIC_LITERAL
  | COLUMN_LITERAL

COLUMN_LITERAL
  : ID '.' ID
  | ID
```