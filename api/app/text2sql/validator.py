import sqlglot
from sqlglot import exp


ALLOWED_VIEW = "ai_reporting.customer_metrics"

ALLOWED_COLUMNS = {
    "customer_segment",
    "country",
    "total_orders",
    "total_revenue",
    "avg_order_value",
    "days_since_last_order",
    "total_website_events",
    "total_support_tickets",
    "churn_probability",
    "risk_level",
}

ALLOWED_FUNCTIONS = {
    "COUNT", "SUM", "AVG", "MIN", "MAX",
    "ROUND", "COALESCE",
}

ALLOWED_NODE_TYPES = (
    exp.Select,
    exp.From,
    exp.Table,
    exp.Identifier,
    exp.Column,
    exp.Alias,
    exp.Star,
    exp.Literal,
    exp.Null,
    exp.Where,
    exp.Group,
    exp.Order,
    exp.Ordered,
    exp.Limit,
    exp.And,
    exp.Or,
    exp.Not,
    exp.EQ,
    exp.NEQ,
    exp.GT,
    exp.GTE,
    exp.LT,
    exp.LTE,
    exp.Is,
    exp.In,
    exp.Between,
    exp.Add,
    exp.Sub,
    exp.Mul,
    exp.Div,
    exp.Paren,
    exp.Count,
    exp.Sum,
    exp.Avg,
    exp.Min,
    exp.Max,
    exp.Round,
    exp.Coalesce,
    exp.Cast,
    exp.DataType,
    exp.Boolean,
)


def validate_sql(sql: str) -> str:
    if not isinstance(sql, str) or len(sql) > 5000:
        raise ValueError("Invalid SQL length")

    statements = sqlglot.parse(sql, read="postgres")

    if len(statements) != 1:
        raise ValueError("Exactly one statement is required")

    tree = statements[0]

    if not isinstance(tree, exp.Select):
        raise ValueError("Only SELECT statements are allowed")

    # Disallow nested SELECTs, including scalar subqueries.
    if len(list(tree.find_all(exp.Select))) != 1:
        raise ValueError("Subqueries are not allowed")

    tables = list(tree.find_all(exp.Table))

    if len(tables) != 1:
        raise ValueError("Exactly one approved view is required")

    table = tables[0]
    full_name = f"{table.db}.{table.name}".lower()

    if full_name != ALLOWED_VIEW or table.catalog:
        raise ValueError("Table is not allowlisted")

    if any(
        isinstance(node, exp.Star)
        for node in tree.expressions
    ):
        raise ValueError("SELECT * is not allowed")

    for node in tree.walk():
        if isinstance(node, exp.Func):
            if not isinstance(node, exp.Cast) and node.sql_name().upper() not in ALLOWED_FUNCTIONS:
                raise ValueError(
                    f"Function not allowed: {node.sql_name()}"
                )
        elif not isinstance(node, ALLOWED_NODE_TYPES):
            raise ValueError(
                f"SQL construct not allowed: {type(node).__name__}"
            )

    for column in tree.find_all(exp.Column):
        if column.name.lower() not in ALLOWED_COLUMNS:
            # Allow ORDER BY aliases from SELECT expressions.
            aliases = {
                item.alias.lower()
                for item in tree.expressions
                if item.alias
            }
            if column.name.lower() not in aliases:
                raise ValueError(
                    f"Column not allowed: {column.name}"
                )

        if column.db or column.catalog:
            raise ValueError("Qualified columns not supported")

        if column.table and column.table.lower() not in {
            table.name.lower(),
            (table.alias or "").lower(),
        }:
            raise ValueError("Unknown table qualifier")

    # Enforce a maximum number of result rows.
    limit = tree.args.get("limit")
    if limit is not None:
        value = limit.expression
        if not isinstance(value, exp.Literal) or not value.is_int:
            raise ValueError("LIMIT must be a positive integer")
        if int(value.this) <= 0:
            raise ValueError("LIMIT must be a positive integer")
    if limit is None or int(limit.expression.this) > 100:
        tree.set(
            "limit",
            exp.Limit(expression=exp.Literal.number(100)),
        )

    return tree.sql(dialect="postgres")
