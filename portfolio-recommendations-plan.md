# Portfolio Recommendations Plan

Design doc for rebuilding portfolio optimization into a learning system.
Tracks research-grounded recommendations, observes outcomes, and refines
signal weights over time.

Status: Planning. Last updated 2026-04-23.

## Goal

Recommendations users trust and that improve over time. Grounded in validated
portfolio theory. Transparent about which signals drove them. Refined by
observed outcomes in our own universe.

## Current state diagnosis

Today's optimizer: `financials/portfolio_risk.py::compute_efficient_frontier`.
Monte Carlo of 1000 random weight vectors across current holdings. Picks
highest-Sharpe allocation. Four quality problems:

1. **Historical 1y returns as expected-return input.** Classic Markowitz
   pitfall. Overweights whatever ran last year. Unstable out of sample.
2. **No constraints.** No min/max per position, no sector caps, no turnover
   limits. Output can be 70% in one ticker. Users will not act on it.
3. **Does not use Alpha Score.** `/dashboard` tells users to trust a 13-factor
   composite. `/portfolio` ignores it. Two tools, disconnected advice.
4. **Sharpe-only, no narrative.** Sortino, VaR, drawdown computed elsewhere
   and thrown away here. Flat table of trades with no why, no confidence.

## Target architecture

Six-part loop. Each piece independently useful. Together they form a learning
system.

1. **Signals.** Alpha Score sub-scores. Already built.
2. **Views optimizer.** Black-Litterman with Alpha Score as views and
   market-cap prior. Constrained (min/max weights, sector caps, turnover).
3. **Recommendation store.** SQLite table alongside `data/alpha.db`. Every
   rec snapshotted with input vector: sub-scores driving each position,
   factor weights at issue, regime tags (VIX, yield curve, sector cycle),
   expected return and vol at issue.
4. **Outcome observer.** Scheduled job pulls prices at 30/90/180/365-day
   horizons. Measures realized return vs do-nothing counterfactual and
   equal-weight benchmark.
5. **Factor attribution.** Decompose each rec's realized P&L into
   contributions by factor (value, momentum, quality, etc.).
6. **Weight learner.** Updates `factor_weights` in `alpha.db` with
   regularization toward academic priors. Sample-size gated.
   Regime-conditioned.

## Research grounding

Validated (safe priors):

- **Value, momentum, quality, low-vol, size.** Fama-French 3/5 factor,
  Carhart. Robust across decades.
- **Black-Litterman (1992).** Tames mean-variance instability by blending
  views with equilibrium prior.
- **Risk parity.** Alternative allocation that does not rely on return
  forecasts.

Weaker or mixed evidence in our current Alpha Score:

- **Analyst revisions.** Works in some samples, not others.
- **Insider transactions.** Mixed. Hard to trade on post-filing.
- **Buyback announcements.** Moderate, decays fast.

Feedback loop will show which signals pay out in our specific universe.

## Phases

### Phase 1 (~2 weeks): Foundation

Forward-looking side. No outcome tracking yet.

- [ ] New SQLite table `portfolio_recommendations` in `data/alpha.db`:
      rec id, timestamp, portfolio fingerprint, holdings vector, suggested
      weights, expected return/vol, constraint parameters, regime tags,
      per-position factor attribution.
- [ ] Black-Litterman optimizer in new `financials/portfolio_optimizer.py`
      (keep `portfolio_risk.py` for its existing concerns). Alpha Score
      sub-scores feed views. Market-cap weights as prior. Omega (view
      confidence) seeded from historical factor track record.
- [ ] Constraint layer: min/max position weights (e.g. 1%-25%), sector caps,
      turnover limit (max X% shift per rec), min trade size (ignore shifts
      under ~1%).
- [ ] Capture factor attribution at rec time: for each position change,
      decompose into factor contributions.
- [ ] UI rewrite in `templates/partials/portfolio_optimizer.html`: per
      recommended trade, show driving factors and academic base rates.
      Confidence indicator. Clear narrative.
- [ ] Wire into existing `/api/portfolio/widget/optimizer` endpoint.

### Phase 1.5 (~1 week): Recommend new positions

Extends the optimizer beyond reallocating existing holdings. Combines two
approaches: (A) sector gap detection suggests ETFs or stocks for under-
weight sectors; (B) marginal Sharpe contribution screening ranks
candidates from the ~193-ticker Alpha universe by their improvement to
portfolio Sharpe. Output is a short list of suggested adds, not a
universe-wide re-optimization. Must ship before Phase 2.

- [ ] Define candidate universe (Alpha-scored seed + broad-market ETFs).
- [ ] Sector gap detector using effective sector totals.
- [ ] Marginal Sharpe contribution calculator per candidate.
- [ ] Combined ranking with explanations (gap vs Sharpe vs both).
- [ ] UI: separate "Consider Adding" section in optimizer widget.

### Phase 2 (~1-2 weeks): Observation

- [ ] New SQLite table `recommendation_outcomes`: rec id, horizon
      (30/90/180/365), realized return (portfolio, do-nothing counterfactual,
      equal-weight benchmark), per-factor realized contribution.
- [ ] Outcome observer: scheduled job, same pattern as `alpha_collector`
      cron. Pulls prices, computes realized returns, writes outcomes.
      Handles yfinance gaps and retries.
- [ ] `/portfolio/history` view: list of past recs with realized
      performance, factor attribution, regime context.

### Phase 3 (months 2-6+): Accumulate

Passive. Do not change weights. Let sample size grow. Watch history view for
obvious bugs in attribution math.

### Phase 4 (once N is sufficient): Close the loop

- [ ] Sample-size threshold per factor (conservative: 30+ observations per
      factor per regime).
- [ ] Regularized factor weight updates in `alpha.db.factor_weights`.
      Shrinkage toward academic prior, tunable strength.
- [ ] Regime-conditioned weights: separate factor weights per regime bucket
      (low/med/high VIX, yield curve state).
- [ ] Cross-validation: split history into train/test. Only adjust weights
      if out-of-sample improvement holds.

## Hard truths

- **No meaningful feedback-loop signal for 6-12 months.** Sample size matters.
  Do not close the loop early.
- **Overfitting is the primary failure mode.** Tuning weights on 20
  observations makes things worse. Sample-size gate and shrinkage are
  non-negotiable.
- **Regime-tagging is required.** Factors reverse across regimes. Flat
  averaging masks this.
- **yfinance reliability is a known risk.** Outcome observer must handle
  gaps and retries.

## Scoping decisions (resolved 2026-04-23)

- **Portfolio identity:** persistent client id via localStorage UUID. No auth
  yet. Each recommendation is stamped with `client_id` plus a full holdings
  snapshot. Upgrade to real accounts later if the app grows.
- **Rec scope:** reallocate-only. Cannot suggest new tickers in Phase 1.
  Ticker discovery is a separate feature (universe selection,
  diversification rules, explain-why). Ship the core loop first.
- **Confidence in UI:** both. Headline 0-100 score up top, factor breakdown
  in a "why this rec" expandable section. Curious users can dig in.
- **Tax lot awareness:** not now. Do not stub the data model. Add fields via
  migration when it becomes a real requirement.

## Parked for later

- Tax lot awareness (recommend based on lot-level cost basis to realize
  losses, avoid realizing gains). Add when product intent firms up.
- Real user accounts and multi-portfolio tracking per user.

## Changelog

- 2026-04-23: Initial plan drafted from diagnosis and architecture
  conversation.
- 2026-04-23: Resolved 4 open questions. Moved tax-lot to parked list.
- 2026-04-23: Phase 1 complete. Built recommendation store, persistent
  client id, Black-Litterman optimizer (numpy, 3y data, tau=0.05,
  delta=2.5, tilt=0.03, vs-neutral-50 demeaning), constraint layer
  (pos 2-25%, sector 40%, turnover 30%, max_vol_ratio 1.10), factor
  attribution, regime capture (VIX, 10y-3m spread), confidence and
  diversification scores, two-mode UI toggle (Diversification /
  Return-Max), wired endpoint with persistence.
- 2026-04-23: ETF handling. Detect via yfinance quoteType, pin Alpha at
  50, exclude from sector cap, models broad ETFs as evenly spread
  across 11 GICS sectors in diversification score, effective sector
  cap counts ETF contribution toward the 40% target. Vol cap bug fix:
  closed-form variance-minimizing blend when both endpoints exceed
  cap. Optimizer pulled out of Risk tab to top of results page.
  Improved loading state with spinner and explanatory text. Added
  Phase 1.5 (new-position recs) to the plan.
- 2026-04-23: Phase 1.5 complete. Three suggestion methods shipped:
  Sector Gaps (recommend SPDR sector ETF for under-weighted GICS
  sectors), Marginal Sharpe (top 8 candidates from ~206-name universe
  by Sharpe contribution from a 2% slice), Holistic (full-universe
  B-L re-optimization with position/sector caps, surfaces NEW positions
  >=5% weight and >=$500). High Conviction badge for cross-method
  matches. Data freshness footer. Rate-limit banner. Stock/ETF/Sector
  ETF pills.
- 2026-04-23: Page restructure. Removed 4 redundant sections (Dividend
  Income Projector, Compound Growth Projection, Mosaic Scores widget,
  server-side Diversification Gaps). Removed 3 pill nav tabs and the
  toggleWidgetGroup JS. Reordered to decision-flow hierarchy: TOP =
  Summary, Health, AI Commentary, Optimizer, Consider Adding; MIDDLE
  = Diversification Insights, Concentration Alerts, Historical, Risk
  Dashboard, All Holdings; EXPANDABLE behind one button = 14 advanced
  widgets. Page went from ~28 sections / 1046 lines to 11 visible
  sections / 663 lines.
- 2026-04-23: Phase 2 complete. Outcome data layer
  (recommendation_outcomes table with INSERT OR REPLACE for idempotent
  re-measurement). Outcome observer (financials/outcome_observer.py
  with CLI: realized return, do-nothing counterfactual, SPY benchmark,
  equal-weight benchmark, per-factor linear Brinson-style attribution,
  graceful yfinance gap handling). Portfolio history view at
  /portfolio/history with per-rec collapsible cards showing outcome
  table per horizon plus the "Rec - Counterfactual" learning signal
  column. Phase 3 (passive accumulation) starts now; Phase 4 (weight
  learner) waits for sufficient sample size.
- 2026-04-24: Backtest harness shipped (financials/backtest.py,
  alpha_historical.py, /backtest page). Alpha-Lite point-in-time
  reconstruction of 5 price-based factors (momentum, technical,
  industry_cycle, macro, earnings_surprise); other 8 held at
  neutral 50. 2020-2024 monthly over 3 portfolio templates =
  162 recs / 648 outcomes. Hit rates 54-58% vs counterfactual AND
  SPY across all horizons (optimizer is net-positive). Factor IC
  at 90d: macro -0.225 (strongly inverted), industry_cycle +0.056,
  momentum +0.017, technical -0.036, earnings_surprise null. Macro
  downweighted from 0.07 to 0.01 (0.68% effective weight) pending
  Phase 4 learner. Ledoit-Wolf covariance shrinkage replaces simple
  ridge. TC gate (10bps) filters nuisance trades. Market-cap prior
  uses median substitution instead of silent equal-weight fallback.
