# Project Agenda

This document outlines outstanding tasks and potential next steps for enhancing the Opening Range Breakout Dashboard.

## Short-Term Tasks

- [ ] Create or update `requirements.txt` to document all necessary Python dependencies.
- [ ] Provide sample datasets in the `data/` directory so new users can test the app without additional preparation.
- [ ] Expand the machine-learning documentation in `ml_models.py`, covering the training workflow, evaluation metrics, and pickle export process.

## Mid-Term Ideas

- [ ] Extend the strategy backtester with risk and money management metrics (e.g., maximum drawdown, Sharpe ratio).
- [ ] Integrate an automated data ingestion pipeline (database or API) to keep the CSV files current.
- [ ] Add multilingual support (English/German) within the Streamlit app.

## Long-Term Vision

- [ ] Build a continuous training pipeline for machine-learning models, including evaluation and versioning.
- [ ] Provide a deployment setup (Docker or cloud) to host the dashboard in production.
- [ ] Develop additional modules for related trading strategies (e.g., VWAP reversion, opening drive).
