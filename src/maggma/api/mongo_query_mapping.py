# Table mapping query operator to mongo operator
query_mapping = {
    "eq": "$eq",
    "not_eq": "$not",
    "lt": "$lt",
    "gt": "$gt",
    "in": "$in",
    "not_in": "$nin",
}

supported_types = [str, int, float]
