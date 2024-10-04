import pandas as pd
from scripts.bilateral import get_bilateral_health_oda


def health_with_and_without_covid(
    prices: str = "constant",
    base_year: int = 2022,
    start_year: int = 2015,
    end_year: int = 2022,
) -> pd.DataFrame:

    grouper = ["year"]

    # Get the data for health
    health = (
        get_bilateral_health_oda(
            start_year=start_year,
            end_year=end_year,
            prices=prices,
            base_year=base_year,
            exclude_covid=False,
        )
        .groupby(grouper, observed=True, dropna=False)["value"]
        .sum()
        .reset_index()
        .assign(indicator="Health ODA")
    )

    health_without_covid = (
        get_bilateral_health_oda(
            start_year=start_year,
            end_year=end_year,
            prices=prices,
            base_year=base_year,
            exclude_covid=True,
        )
        .groupby(grouper, observed=True, dropna=False)["value"]
        .sum()
        .reset_index()
        .assign(indicator="Health ODA (without COVID)")
    )

    data = pd.concat([health, health_without_covid], ignore_index=True)
    data = data.pivot(index="year", columns="indicator", values="value").reset_index()

    return data


if __name__ == "__main__":
    df = health_with_and_without_covid()
