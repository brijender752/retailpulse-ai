import random

from faker import Faker

fake = Faker("en_US")


PRODUCT_CATALOG = {
    "Electronics": [
        "Laptop",
        "Monitor",
        "Keyboard",
        "Mouse",
        "Headphones",
        "Webcam",
        "Tablet",
        "Smartphone",
    ],
    "Home": [
        "Coffee Maker",
        "Blender",
        "Vacuum Cleaner",
        "Air Purifier",
        "Desk Lamp",
        "Office Chair",
    ],
    "Clothing": [
        "T-Shirt",
        "Jeans",
        "Jacket",
        "Sneakers",
        "Hoodie",
        "Dress",
    ],
    "Sports": [
        "Yoga Mat",
        "Dumbbells",
        "Running Shoes",
        "Football",
        "Tennis Racket",
    ],
    "Books": [
        "Technology Book",
        "Business Book",
        "Novel",
        "Science Book",
        "Programming Book",
    ],
}


BRANDS = [
    "TechPro",
    "Nova",
    "Vertex",
    "Apex",
    "Zenith",
    "Prime",
    "Orbit",
    "Nexus",
]


def generate_products(count: int) -> list[dict]:
    products = []

    categories = list(PRODUCT_CATALOG.keys())

    for _ in range(count):

        category = random.choice(categories)

        subcategory = random.choice(
            PRODUCT_CATALOG[category]
        )

        cost = round(
            random.uniform(10, 500),
            2,
        )

        margin = random.uniform(1.15, 2.5)

        price = round(
            cost * margin,
            2,
        )

        products.append(
            {
                "product_name": f"{random.choice(BRANDS)} {subcategory}",
                "category": category,
                "subcategory": subcategory,
                "brand": random.choice(BRANDS),
                "price": price,
                "cost": cost,
                "supplier_id": random.randint(1, 100),
                "inventory_quantity": random.randint(0, 1000),
            }
        )

    return products