# Table mapping query operator to mongo operator
query_mapping = {
    "eq": "$eq",
    "not_eq": "$not",
    "lt": "$lt",
    "gt": "$gt",
    "in": "$in",
    "not_in": "$nin",
}


def translate_operator(k: str):
    """
    A utility function that seperates name with operator
    Args:
        k: in the format of NAME_OPERATOR

    Returns:
        return the part after OPERATOR, disregard the NAME

    """
    return k.split("_", 1)[1]


supported_types = [str, int, float]
