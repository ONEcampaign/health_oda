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
    end_year: int = 2023,
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


if __name__ == "__main__":
    df = read_crs_eui(2022)
    df = df.query("donor_code == 918")
    df = df.pipe(filter_covid_sectors)
    df["covid"] = df.covid_k | df.covid_p | df.covid_t

    df = df.loc[lambda d: d.covid]
