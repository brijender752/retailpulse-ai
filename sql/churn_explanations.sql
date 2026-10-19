CREATE SCHEMA IF NOT EXISTS serving;

CREATE TABLE IF NOT EXISTS serving.churn_explanations (
    customer_id BIGINT NOT NULL,
    prediction_date TIMESTAMPTZ NOT NULL,
    model_version TEXT NOT NULL,
    output_space TEXT NOT NULL,
    base_value DOUBLE PRECISION,
    feature_contributions JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (
        customer_id,
        prediction_date,
        model_version
    )
);
