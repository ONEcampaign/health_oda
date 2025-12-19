from typing import Optional

import pandas as pd
from oda_data import read_crs, set_data_path
from oda_data.classes.oda_data import READERS

from scripts import config
from scripts.common import (
    remap_covid_keyword,
    remap_covid_purpose,
    remap_covid_trust_fund,
    get_health_oda_indicator,
    flag_covid_keyword,
    flag_covid_purpose,
    flag_covid_trust_fund,
    filter_covid_sectors,
)

set_data_path(config.Paths.raw_data)


def read_crs_remap_covid(years):
    data = read_crs(years)

    data = (
        data.pipe(remap_covid_keyword)
        .pipe(remap_covid_purpose)
        .pipe(remap_covid_trust_fund)
    )

    return data


def read_crs_eui(years):
    data = read_crs(years)

    data = (
        data.pipe(flag_covid_keyword)
        .pipe(flag_covid_purpose)
        .pipe(flag_covid_trust_fund)
    )

    return data


def monkey_patch_read_crs():
    READERS["crs"] = read_crs_remap_covid


def get_imputed_multilateral_health_oda(
    start_year: int = 2000,
    end_year: int = 2024,
    by_recipient: bool = False,
    prices: str = "current",
    currency: str = "USD",
    base_year: Optional[int] = None,
    exclude_covid: bool = False,
) -> pd.DataFrame:

    if exclude_covid:
        monkey_patch_read_crs()
    else:
        READERS["crs"] = read_crs

    data = get_health_oda_indicator(
        indicator="imputed_multi_flow_disbursement_gross",
        start_year=start_year - 2,
        end_year=end_year,
        prices=prices,
        currency=currency,
        base_year=base_year,
    ).loc[lambda d: d.year >= start_year]

    data["indicator"] = "imputed_multilateral_health_oda"
    data["value"] = data["value"].astype(float)

    grouper = ["year", "indicator", "donor_code", "prices"]
    if by_recipient:
        grouper.append("recipient_code")

    data = (
        data.groupby(grouper, observed=True, dropna=False)["value"].sum().reset_index()
    )

    return data


def imputed_health_with_and_without_covid(
    prices: str = "constant",
    base_year: int = 2024,
    start_year: int = 2015,
    end_year: int = 2024,
) -> pd.DataFrame:

    grouper = ["year", "donor_code"]

    # Get the data for health
    health = (
        get_imputed_multilateral_health_oda(
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
        get_imputed_multilateral_health_oda(
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
    df = imputed_health_with_and_without_covid(
        start_year=2019, base_year=2024, prices="constant"
    )

    dac_df = df.loc[lambda d: d.donor_code.isin(list(dac))]
