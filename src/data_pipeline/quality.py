def is_valid_customer_id(customer_id: str | None) -> bool:
    if customer_id is None:
        return False

    if customer_id.strip() == "":
        return False

    return True