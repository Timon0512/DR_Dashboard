# Opening Range Breakout Dashboard

This repository contains an interactive Streamlit application for analyzing Opening Range Breakout (ORB) strategies across various futures and FX markets. The dashboard helps traders review historical session data, assess probabilities, and validate their trading assumptions using statistical metrics and machine-learning outputs.

## Key Features

- **Interactive filters**: Choose the symbol, trading session (New York, London, Tokyo), Opening Range duration (30 or 60 minutes), and refine the dataset by weekday, month, or year.
- **Breakout statistics**: Track metrics for range breakouts, range holds, retracements, expansions, and closes outside the Opening Range.
- **Distribution analytics**: Plotly visualizations for breakout windows, retracement and expansion levels, including cumulative probability curves.
- **Session model overview**: Display of detected session models (e.g., Strong Uptrend, Expansion) with illustrative images and scenario analysis derived from past sessions.
- **Strategy backtesting**: Configure entry, exit, and stop parameters to evaluate historical performance based on the selected filters.
- **Machine-learning insights**: Load preconfigured models and scalers to classify the current session using persisted pickle files.
- **Data transformations**: `orb_calculations.py` builds and updates the underlying CSV datasets from 5-minute candles.

## Project Structure

```
DR_Dashboard/
├── data/                 # CSV files grouped by symbol, session, and Opening Range duration
├── ml_models/            # Trained ML models and scalers (.pickle)
├── pictures/             # Images used to describe session models inside the dashboard
├── session_models/       # Additional data describing session models
├── streamlit_app.py      # Main Streamlit application
├── orb_calculations.py   # Script to create or update ORB datasets
├── ml_models.py          # Helper functions for ML workflows
└── README.md             # This document
```

## Prerequisites

- Python 3.10 or newer (recommended)
- Virtual environment (e.g., `venv` or `conda`)
- Installed dependencies: `streamlit`, `pandas`, `numpy`, `plotly`, `polars`, `scikit-learn`, `python-dateutil`, plus any requirements of your ML models.

> **Note:** Add further packages according to your local environment and modeling workflow.

## Installation & Launch

1. Clone the repository:
   ```bash
   git clone <repo-url>
   cd DR_Dashboard
   ```
2. Create and activate a virtual environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Windows: .venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
   If no `requirements.txt` is available, install the necessary packages manually.
4. Start the Streamlit app:
   ```bash
   streamlit run streamlit_app.py
   ```
5. Open the displayed local URL in your browser to use the dashboard.

## Data Sources & Format

- CSV files in `data/` are semicolon-separated (`;`) and use a date index.
- Timestamps (e.g., `breakout_time`, `max_retracement_time`, `max_expansion_time`) are stored in microseconds and converted to the appropriate time zone (`America/New_York`) inside the app.
- Use `orb_calculations.py` to generate new ORB datasets from raw 5-minute candle data or to refresh existing files.

## Machine-Learning Models

- Models and scalers are expected as pickle files inside `ml_models/`, following the naming pattern `<symbol>_<session>_simple_confirmation_bias_model.pickle`.
- The app automatically checks whether a matching model exists for the selected symbol and session.
- Leverage `ml_models.py` to organize training, evaluation, and export of custom models.

## Further Development

- Add additional symbols or sessions by contributing new CSV datasets.
- Extend `streamlit_app.py` with new Plotly visualizations or metrics.
- Adapt the strategy backtester to model your specific trading rules.

## Support

If you have questions or suggestions, please open an issue or submit a pull request.
