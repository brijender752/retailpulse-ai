select payment_id,amount from {{ ref('fact_payments') }} where amount < 0
