# hockeypreds
NHL-Hockey-Predictions

## Prospective Hockey Modeling

This repository demonstrates a streamlined approach to building and analyzing predictive models for hockey games. While the foundation of this project is shared here, some proprietary details and betting strategies remain private.

## Overview

- **Data Pipeline**: Data is pulled from the Official NHL API and preprocessed into a structured format for modeling.
- **Modeling Approach**: General machine learning and statistical methods are used to predict player prop bets, game outcomes (moneyline), and game spreads (puckline).
- **Sample Results**: Provides a high-level look at model outputs and performance metrics (win probabilities, expected goals, etc.).

## Why This Project?

The goal is to showcase practical skills in data engineering, model development, and hockey-specific analytics. It serves as a partial portfolio piece for employers and collaborators, illustrating real-world sports analytics concepts without exposing all trade secrets.

## Getting Started

1. **Clone the Repository** (or fork it) to your local machine:

   ```bash
   git clone https://github.com/schwill2018/hockeypreds.git
   ```

2. **Install R Package Requirements**:

   ```r
   install.packages(c("dplyr", "tidymodels", "tidyverse", "ggplot2"))
   ```

3. **Explore** the scripts in the `preprocessing` folder. These include:
   - `api_pull.R`: Pulls data from the Official NHL API.
   - `data_preprocessing.R`: Prepares the raw data for modeling.  

4. **Explore** the scripts in the `analysis` folder. These include:
   - `earned_point.R`: Predicts player prop bets.
   - `earned_assist.R`: Predicts player prop bets.
   - `earned_goal.R`: Predicts player prop bets.
   - `team_game_won_glm.R`: Predicts team-level win outcomes (moneyline).
   - `team_game_glm_spread.R`: Predicts team puckline outcomes.
## What’s Included

- **Preprocessed 2023–2024 Dataset**.
- **Scripts** for API calls, data cleaning, and modeling for different bet types.
- **Example Output** to illustrate model predictions and structure.

## What’s Not Included

- **Full Betting Strategy** logic and final decision rules (proprietary).
- **Live/Current Data Feeds** or fully up-to-date datasets beyond the included season.

## Future Work

- Expanded modeling features and temporal dynamics.
- Team-level fatigue tracking, travel effects, and matchup-based adjustments.
- Live updating utility models and deployment.

## Contributing

This project isn’t actively seeking external contributions at the moment. However, if you have suggestions or questions, feel free to open an issue.

## License

This project is licensed under [choose an appropriate license], with proprietary portions excluded from public release.

## Contact

Questions or interested in collaborating? Reach out via:
- **Email**: [schneiderw94@gmail.com](mailto:schneiderw94@gmail.com)
- **LinkedIn**: [Will Schneider](https://www.linkedin.com/in/willschneider214/)
