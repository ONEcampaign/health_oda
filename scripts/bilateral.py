from typing import Optional

import pandas as pd
from oda_data import set_data_path

from scripts import config
from scripts.common import (
    get_health_oda_indicator,
    remove_covid_keyword,
    remove_covid_purpose,
    remove_covid_trust_fund,
)

set_data_path(config.Paths.raw_data)


def get_bilateral_health_oda(
    start_year: int = 2000,
    end_year: int = 2023,
    by_recipient: bool = False,
    prices: str = "current",
    currency: str = "USD",
    base_year: Optional[int] = None,
    exclude_covid: bool = False,
    additional_groupers: Optional[list[str]] = None,
) -> pd.DataFrame:
    """"""
    data = get_health_oda_indicator(
        indicator="crs_bilateral_flow_disbursement_gross",
        start_year=start_year,
        end_year=end_year,
        prices=prices,
        currency=currency,
        base_year=base_year,
    )

    data["indicator"] = "bilateral_health_oda"
    data["value"] = data["value"].astype(float)

    if exclude_covid:
        data = remove_covid_keyword(data)
        data = remove_covid_purpose(data)
        data = remove_covid_trust_fund(data)

    grouper = ["year", "indicator", "donor_code", "prices"] + (
        additional_groupers or []
    )

    if by_recipient:
        grouper.append("recipient_code")

    data = (
        data.groupby(grouper, observed=True, dropna=False)["value"].sum().reset_index()
    )

    return data


if __name__ == "__main__":
    df = get_bilateral_health_oda(2013, 2023, by_recipient=False)
