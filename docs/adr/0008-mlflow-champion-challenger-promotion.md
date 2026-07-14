# ADR-0008: Model promotion is a CI gate over MLflow aliases, not a human decision

- **Status:** Accepted
- **Date:** 2026-07-14
- **Deciders:** data-platform

## Context

Two models are in production use: a PyTorch two-tower retrieval model with a churn head, and a
TensorFlow sales/demand forecaster. Both retrain on a schedule. The failure mode we are designing
against is the familiar one: a retrained model is promoted because it is new, the offline metric
was eyeballed in a notebook, and the regression is discovered by the business two weeks later.

## Decision

The registry is the promotion mechanism, and the gate is code:

- **Tracking backend follows the environment.** Local file store on `dev` (no network, no cost);
  **Databricks-managed MLflow** on `qa` and `prod` (shared, audited).
- **Aliases, not stages.** `dev` → the run is logged only. `qa` → the model is registered and gets
  the `@challenger` alias. `prod` → the challenger is promoted to `@champion` **only if it beats the
  incumbent champion on the holdout metric by at least `MIN_IMPROVEMENT`**.
- The gate runs as an Airflow `ShortCircuitOperator` (`dag_ml_training`:
  `train → evaluate_challenger → promotion_gate → promote → score`) and as a CD step. If the
  challenger does not clear the bar, the DAG short-circuits and the champion stays. Nobody has to
  remember not to promote it.
- Serving (`ml/serving/batch_inference.py`) always resolves `@champion`, never a version number, so
  rollback is an alias move.

## Consequences

**Positive.** "The model got worse" stops being a class of incident that reaches production. Rollback
is instantaneous and does not require a redeploy. Every promotion is attributable to a run, a
metric, and a commit.

**Negative.** A genuinely better model that improves the metric by less than `MIN_IMPROVEMENT` will
not be promoted automatically — the threshold is a deliberate bias towards stability, and moving it
is a config change with a review attached.

**Neutral.** The holdout metric becomes the definition of "better". Choosing it badly is now a
first-order design problem, which is where it belongs (it is chosen chronologically, never
randomly, for the forecaster — a random split would leak the future).

## Alternatives considered

- **Manual promotion in the MLflow UI.** Works until the person who knows the threshold is on
  holiday. Rejected.
- **Always promote the newest model.** Turns every training bug into a production bug. Rejected.
- **Shadow/canary serving in production.** The correct long-term answer for the online path, and
  overkill for a batch-scored churn model. Deferred, not rejected — the alias mechanism is what a
  canary would be built on.
