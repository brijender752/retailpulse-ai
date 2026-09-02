import random


def generate_inventory(
    products: list[dict],
) -> list[dict]:

    inventory = []

    for product in products:

        inventory.append(
            {
                "product_id": product["product_id"],
                "warehouse_id": random.randint(
                    1,
                    10,
                ),
                "quantity": random.randint(
                    0,
                    1000,
                ),
                "reserved_quantity": random.randint(
                    0,
                    100,
                ),
            }
        )

    return inventory