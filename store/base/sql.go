package base

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// validIdentifier matches safe SQL identifiers (alphanumeric and underscores).
var validIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// ValidIdentifier returns true if name is a safe SQL identifier.
func ValidIdentifier(name string) bool {
	return validIdentifier.MatchString(name)
}

// QueryBuilder helps construct SQL queries with dynamic conditions.
// It handles parameter numbering automatically for PostgreSQL-style placeholders ($1, $2, etc.).
//
// QueryBuilder is designed to eliminate the common boilerplate of building
// WHERE clauses with conditional filters. It automatically handles:
//   - Parameter numbering ($1, $2, $3...)
//   - Conditional inclusion of filters
//   - IN clause generation
//   - LIMIT/OFFSET appending
//
// Example:
//
//	qb := base.NewQueryBuilder()
//	qb.AddIfNotEmpty("event_name = $%d", filter.EventName)
//	qb.AddIfNotZero("created_at >= $%d", filter.StartTime)
//	qb.AddIfPositive("retry_count <= $%d", filter.MaxRetries)
//	qb.AddIn("status", filter.Statuses)
//	qb.AddRawIf(filter.ExcludeRetried, "retried_at IS NULL")
//
//	query, args := qb.Build("SELECT * FROM events %s ORDER BY created_at")
//	// query: "SELECT * FROM events WHERE event_name = $1 AND status IN ($2, $3) ORDER BY created_at"
//	// args: ["order.created", "pending", "active"]
type QueryBuilder struct {
	conditions []string
	args       []any
	argNum     int
}

// NewQueryBuilder creates a new query builder starting at $1.
func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{argNum: 1}
}

// NewQueryBuilderFrom creates a new query builder starting at a specific argument number.
// Useful when some arguments are already added to the query.
func NewQueryBuilderFrom(startArg int) *QueryBuilder {
	return &QueryBuilder{argNum: startArg}
}

// Add adds a condition with a parameterized value.
// The condition should contain %d where the parameter number goes.
// Example: qb.Add("name = $%d", "John")
func (qb *QueryBuilder) Add(condition string, value any) *QueryBuilder {
	qb.conditions = append(qb.conditions, fmt.Sprintf(condition, qb.argNum))
	qb.args = append(qb.args, value)
	qb.argNum++
	return qb
}

// AddIf adds a condition only if the predicate is true.
func (qb *QueryBuilder) AddIf(predicate bool, condition string, value any) *QueryBuilder {
	if predicate {
		return qb.Add(condition, value)
	}
	return qb
}

// AddIfNotEmpty adds a condition only if the string value is not empty.
func (qb *QueryBuilder) AddIfNotEmpty(condition string, value string) *QueryBuilder {
	if value != "" {
		return qb.Add(condition, value)
	}
	return qb
}

// AddIfNotZero adds a condition only if the time value is not zero.
func (qb *QueryBuilder) AddIfNotZero(condition string, value time.Time) *QueryBuilder {
	if !value.IsZero() {
		return qb.Add(condition, value)
	}
	return qb
}

// AddIfPositive adds a condition only if the value is positive.
func (qb *QueryBuilder) AddIfPositive(condition string, value int) *QueryBuilder {
	if value > 0 {
		return qb.Add(condition, value)
	}
	return qb
}

// AddIfPositiveDuration adds a condition only if the duration is positive.
func (qb *QueryBuilder) AddIfPositiveDuration(condition string, value time.Duration) *QueryBuilder {
	if value > 0 {
		return qb.Add(condition, value.Milliseconds())
	}
	return qb
}

// AddRaw adds a condition without any parameters.
// Use this for static conditions like "deleted_at IS NULL".
func (qb *QueryBuilder) AddRaw(condition string) *QueryBuilder {
	qb.conditions = append(qb.conditions, condition)
	return qb
}

// AddRawIf adds a raw condition only if the predicate is true.
func (qb *QueryBuilder) AddRawIf(predicate bool, condition string) *QueryBuilder {
	if predicate {
		return qb.AddRaw(condition)
	}
	return qb
}

// AddIn adds an IN condition with multiple values.
// Example: qb.AddIn("status", []string{"active", "pending"})
func (qb *QueryBuilder) AddIn(column string, values []string) *QueryBuilder {
	if len(values) == 0 {
		return qb
	}

	placeholders := make([]string, len(values))
	for i, val := range values {
		placeholders[i] = fmt.Sprintf("$%d", qb.argNum)
		qb.args = append(qb.args, val)
		qb.argNum++
	}
	qb.conditions = append(qb.conditions, fmt.Sprintf("%s IN (%s)", column, strings.Join(placeholders, ", ")))
	return qb
}

// AddInIf adds an IN condition only if the values slice is not empty.
func (qb *QueryBuilder) AddInIf(column string, values []string) *QueryBuilder {
	if len(values) > 0 {
		return qb.AddIn(column, values)
	}
	return qb
}

// WhereClause returns the WHERE clause string.
// Returns empty string if no conditions were added.
func (qb *QueryBuilder) WhereClause() string {
	if len(qb.conditions) == 0 {
		return ""
	}
	return "WHERE " + strings.Join(qb.conditions, " AND ")
}

// AndClause returns conditions joined by AND without the WHERE keyword.
// Useful for adding to existing WHERE clauses.
func (qb *QueryBuilder) AndClause() string {
	return strings.Join(qb.conditions, " AND ")
}

// Args returns all collected arguments.
func (qb *QueryBuilder) Args() []any {
	return qb.args
}

// ArgNum returns the next argument number.
func (qb *QueryBuilder) ArgNum() int {
	return qb.argNum
}

// HasConditions returns true if any conditions were added.
func (qb *QueryBuilder) HasConditions() bool {
	return len(qb.conditions) > 0
}

// Build constructs the final query by inserting the WHERE clause.
// The baseQuery should contain %s where the WHERE clause goes.
// Example: qb.Build("SELECT * FROM users %s ORDER BY id")
func (qb *QueryBuilder) Build(baseQuery string) (string, []any) {
	return fmt.Sprintf(baseQuery, qb.WhereClause()), qb.args
}

// AppendLimit adds LIMIT and optionally OFFSET to args and returns the clause.
func (qb *QueryBuilder) AppendLimit(limit, offset int) string {
	var clause strings.Builder

	if limit > 0 {
		fmt.Fprintf(&clause, " LIMIT $%d", qb.argNum)
		qb.args = append(qb.args, limit)
		qb.argNum++
	}

	if offset > 0 {
		fmt.Fprintf(&clause, " OFFSET $%d", qb.argNum)
		qb.args = append(qb.args, offset)
		qb.argNum++
	}

	return clause.String()
}

// NullString converts a sql.NullString to a regular string.
// Returns empty string if not valid.
//
// Example:
//
//	var source sql.NullString
//	err := row.Scan(&source)
//	msg.Source = base.NullString(source) // "" if NULL, otherwise the value
func NullString(ns sql.NullString) string {
	if ns.Valid {
		return ns.String
	}
	return ""
}

// NullTime converts a sql.NullTime to a *time.Time.
// Returns nil if not valid.
//
// Example:
//
//	var retriedAt sql.NullTime
//	err := row.Scan(&retriedAt)
//	msg.RetriedAt = base.NullTime(retriedAt) // nil if NULL, otherwise pointer to time
func NullTime(nt sql.NullTime) *time.Time {
	if nt.Valid {
		return &nt.Time
	}
	return nil
}

// NullInt64 converts a sql.NullInt64 to an int64.
// Returns 0 if not valid.
func NullInt64(ni sql.NullInt64) int64 {
	if ni.Valid {
		return ni.Int64
	}
	return 0
}

// NullDurationMs converts a sql.NullInt64 representing milliseconds to time.Duration.
// Returns 0 if not valid.
func NullDurationMs(ni sql.NullInt64) time.Duration {
	if ni.Valid {
		return time.Duration(ni.Int64) * time.Millisecond
	}
	return 0
}

// ToNullString converts a string to sql.NullString.
// Empty strings result in a NULL value.
func ToNullString(s string) sql.NullString {
	return sql.NullString{
		String: s,
		Valid:  s != "",
	}
}

// ToNullTime converts a *time.Time to sql.NullTime.
// Nil pointers result in a NULL value.
func ToNullTime(t *time.Time) sql.NullTime {
	if t == nil {
		return sql.NullTime{}
	}
	return sql.NullTime{
		Time:  *t,
		Valid: true,
	}
}

// StringPtr returns a pointer to the string, or nil if empty.
func StringPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// TimePtr returns a pointer to the time, or nil if zero.
func TimePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}
