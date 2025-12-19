import pandas as pd

from scripts import config
from scripts.bilateral import get_bilateral_health_oda


def health_with_and_without_covid(
    prices: str = "constant",
    base_year: int = 2024,
    start_year: int = 2015,
    end_year: int = 2023,
) -> pd.DataFrame:

    grouper = ["year", "donor_code"]

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
        .assign(indicator="Health ODA (including COVID-19)")
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
        .assign(indicator="Health ODA")
    )

    data = pd.concat([health, health_without_covid], ignore_index=True)

    data = data.pivot(
        index=["year", "donor_code"], columns="indicator", values="value"
    ).reset_index()

    return data


if __name__ == "__main__":
    from oda_data import donor_groupings

    dac = donor_groupings()["dac_countries"]
    df = health_with_and_without_covid(start_year=2019, base_year=2023)

    dac_df = df.loc[lambda d: d.donor_code.isin(list(dac))]

    # df.to_csv(config.Paths.output / "health_constant_by_donor_2024.csv", index=False)
