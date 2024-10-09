from typing import Optional

import pandas as pd
from oda_data import recipient_groupings, ODAData, set_data_path, read_crs

from oda_data.clean_data.schema import OdaSchema

from scripts import config

set_data_path(config.Paths.raw_data)

RECIPIENT_GROUPS = {
    "Developing Countries, Total": None,
    "Africa": recipient_groupings()["african_countries_regional"],
    "Sahel countries": recipient_groupings()["sahel"],
    "Least Developed Countries": recipient_groupings()["ldc_countries"],
    "France priority countries": recipient_groupings()["france_priority"],
}

CURRENCIES: dict = {"USD": "USA", "EUR": "EUI", "GBP": "GBR", "CAD": "CAN"}


def add_income_grouping(df: pd.DataFrame) -> pd.DataFrame:
    """Add the income groupings to the dataframe."""
    from bblocks import add_income_level_column, set_bblocks_data_path

    set_bblocks_data_path(config.Paths.raw_data)

    df = add_income_level_column(
        df, id_column=OdaSchema.RECIPIENT_CODE, id_type="DACCode"
    )

    return df


def get_health_purpose_codes() -> list[str]:
    """Return the purpose codes for health."""
    from oda_data.tools import sector_lists

    sectors = list()

    sectors.extend(sector_lists.health_general)
    sectors.extend(sector_lists.health_basic)
    sectors.extend(sector_lists.health_NCDs)
    sectors.extend(sector_lists.pop_RH)

    return sectors


def filter_health_sectors(df: pd.DataFrame) -> pd.DataFrame:
    # Load the list of sectors
    sectors = get_health_purpose_codes()

    # Filter the dataframe
    return df[df[OdaSchema.PURPOSE_CODE].isin(sectors)].reset_index(drop=True)


def filter_low_income_countries(df: pd.DataFrame) -> pd.DataFrame:
    """Filter the dataframe to include only low-income countries."""
    df = add_income_grouping(df)

    return df.loc[lambda d: d.income_level == "Low income"].reset_index(drop=True)


def filter_african_countries(df: pd.DataFrame) -> pd.DataFrame:
    """Filter the dataframe to include only African countries."""
    return df.loc[
        lambda d: d[OdaSchema.RECIPIENT_CODE].isin(list(RECIPIENT_GROUPS["Africa"]))
    ].reset_index(drop=True)


def remove_covid_keyword(df: pd.DataFrame) -> pd.DataFrame:
    # exclude any rows where the substring 'covid' appears in the keyword column
    return df.loc[lambda d: ~d.keywords.str.contains("covid", case=False, na=False)]


def remove_covid_purpose(df: pd.DataFrame) -> pd.DataFrame:

    return df.loc[lambda d: d.purpose_code != 12264]


def remove_covid_trust_fund(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[lambda d: d.donor_code != 1047]


def remap_covid_keyword(df: pd.DataFrame) -> pd.DataFrame:
    # Any rows with 'covid' in the keyword column will have their purpose code remapped to 160
    df.loc[
        lambda d: d.keywords.str.contains("covid", case=False, na=False), "purpose_code"
    ] = 160

    return df


def remap_covid_purpose(df: pd.DataFrame) -> pd.DataFrame:
    # Any rows with purpose code 12264 will have their purpose code remapped to 160
    df.loc[lambda d: d.purpose_code == 12264, "purpose_code"] = 160

    return df


def remap_covid_trust_fund(df: pd.DataFrame) -> pd.DataFrame:
    # Any rows with donor code 1047 will have their donor code remapped to 160
    df.loc[lambda d: d.donor_code == 1047, "donor_code"] = 160

    return df


GROUPER = [
    "year",
    "indicator",
    "donor_code",
    "recipient_code",
    "purpose_code",
    "keywords",
    "prices",
]


def get_health_oda_indicator(
    indicator: str,
    start_year: int = 2000,
    end_year: int = 2023,
    prices: str = "current",
    currency: str = "USD",
    base_year: Optional[int] = None,
) -> pd.DataFrame:
    # Create an ODAData object
    oda = ODAData(
        years=range(start_year, end_year + 1),
        prices=prices,
        base_year=base_year,
        currency=currency,
    )

    # Load the indicator
    oda.load_indicator(indicator)

    # Get the data, filtered by health sectors
    df = oda.get_data().pipe(filter_health_sectors)

    # Group the data
    grouper = [c for c in GROUPER if c in df.columns]
    df = df.groupby(grouper, dropna=False, observed=True)["value"].sum().reset_index()

    return df
