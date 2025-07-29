# hockeypreds
NHL-Hockey-Predictions

## Prospective Hockey Modeling

This repository demonstrates a rigorous, data-driven approach to forecasting NHL hockey games and evaluating betting strategies. The foundation and core analytics are shared here, while some proprietary logic and strategy details remain private.

## Overview

- **Data Pipeline:** Automatically pulls raw data from the Official NHL API (and bookmaker APIs as needed), processes and engineers features (rolling averages, Elo ratings, travel/fatigue effects) for robust modeling.
- **Modeling Approach:** Implements multiple statistical and machine learning models—Logistic Regression, Random Forest, Neural Networks, XGBoost, and Elo ratings—using prospective (future-proof) cross-validation. Models are stacked for improved calibration and discrimination.
- **Betting & Financial Analysis:** Simulates bet selection based on expected value, edge vs. market, and bookmaker vig. Compares model predictions to market-implied odds, with bankroll simulation and ROI tracking.
- **Results & Evaluation:** Outputs include win probabilities, key metrics (accuracy, ROC-AUC, calibration), ROI summaries, and financial performance for multiple betting strategies.

## Why This Project?

This project showcases practical expertise in:
- **Sports analytics and model engineering:** Complete end-to-end workflow from data acquisition to deployment.
- **Feature engineering for real-world sports:** Including lagged stats, fatigue proxies, Elo-based power ratings, and calibration.
- **Realistic betting analytics:** Market structure analysis, odds mispricing, and simulated profitability.
- **Portfolio piece:** Demonstrates the ability to solve complex, ambiguous data problems for real-world business and analytics contexts.

## Project Structure

- **preprocessing/** – Scripts for data collection (`api_pull.R`), cleaning, and feature engineering (`data_preprocessing.R`).
- **analysis/** – Scripts for modeling and evaluation (player props, team outcomes, betting).
- **sample_outputs/** – Example metrics, ROC curves, calibration plots, bet logs.
- **notebooks/** – Walkthroughs, exploratory notebooks, and documentation (including project outlines).

## Getting Started

1. **Clone the Repository:**

```bash
git clone https://github.com/schwill2018/hockeypreds.git
```

2. **Install R Dependencies:**

```r
install.packages(c("dplyr", "tidymodels", "tidyverse", "ggplot2"))
```

3. **Explore the Scripts:**
   - `preprocessing/` – Data acquisition and cleaning.
   - `analysis/` – Model training, betting simulation, and evaluation.

4. **Example Workflow:**
   - Run `api_pull.R` to download latest game and player data.
   - Run `data_preprocessing.R` for feature engineering.
   - Use modeling scripts in `analysis/` for predictions and financial evaluation.

## What's Included

- **2023–2024 Season Dataset (preprocessed)** for demo and benchmarking.
- **Scripts** for API calls, feature engineering, multiple model types, and betting simulation.
- **Sample outputs** for model metrics, calibration, and financial returns.

## What's Not Included

- **Full live betting logic and proprietary strategy rules**
- **Continuous, live-updated data feeds** beyond the included season.

## Future Work

- Expand XGBoost usage and compare with Random Forests and Neural Nets.
- Live-updating models with Bayesian updating for real-time adaptiveness.
- More advanced fatigue/travel modeling, deeper matchup features.
- Shiny app for public model and odds exploration.
- Expanded bankroll simulation and comparison of betting strategies.

## Contributing

Currently not seeking external contributions, but suggestions and questions are welcome via GitHub issues.

## License

Project is under [choose an appropriate license], with proprietary logic excluded from public release.

## Contact

For feedback, collaboration, or professional inquiries:

- **Email**: [will@brookwaterlabs.com](mailto:will@brookwaterlabs.com)
- **LinkedIn**: [Will Schneider](https://www.linkedin.com/in/willschneider214/)
