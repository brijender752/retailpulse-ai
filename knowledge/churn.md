# RetailPulse Churn Prediction

## Definition

RetailPulse churn prediction estimates whether a customer
is likely to stop purchasing during the prediction horizon.

## Observation Date

An observation date T represents the point at which customer
features are calculated.

Only information available on or before T can be used as
model features.

## Prediction Horizon

RetailPulse uses a 60-day prediction horizon.

If the customer places a qualifying order during:

T+1 through T+60

the churn label is:

churn = 0

If no qualifying order occurs:

churn = 1

## Churn Probability

The churn model produces a probability between 0 and 1.

A higher probability indicates greater model-estimated
churn risk.

The probability is a model prediction and does not guarantee
that the customer will churn.

## Temporal Validation

RetailPulse uses time-based train, validation and test
splits.

Future customer behavior is not used to construct historical
features.

This reduces temporal leakage.