# RetailPulse Recommendation System

## Customer Product Interactions

RetailPulse creates customer-product interactions from
customer behavior such as purchases and product views.

Purchases receive stronger interaction weight than views.

## Product Co-occurrence

Products are considered related when customers interact
with both products.

## Product Similarity

RetailPulse calculates product similarity using customer
interaction patterns.

Similar products can become recommendation candidates for
customers who previously interacted with related products.

## Personalized Similarity

personalized_similarity means that the product was
recommended because it is similar to products previously
associated with the customer.

## Hybrid Personalized

hybrid_personalized combines personalized similarity
information with global product popularity.

## Popular Fallback

popular_fallback is used when personalized recommendation
signals are insufficient.

Globally popular products can then be used as candidates.

## Popular Cold Start

popular_cold_start is used for customers with little or no
historical interaction data.

Because personalization is not yet reliable for these
customers, popular products are used.

## Seen Product Filtering

RetailPulse removes products already seen by the customer
from the final recommendation candidates.

## Top K

The production recommendation system returns up to 10
products per customer.