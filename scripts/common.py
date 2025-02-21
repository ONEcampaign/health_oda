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


def covid_sectors():
    return [
        11110,
        11120,
        11130,
        11182,
        11220,
        11230,
        11231,
        11232,
        11240,
        11250,
        11260,
        11320,
        11330,
        11420,
        11430,
        12110,
        12181,
        12182,
        12191,
        12220,
        12230,
        12240,
        12250,
        12261,
        12262,
        12263,
        12264,
        12281,
        12310,
        12330,
        12340,
        12350,
        12382,
        13010,
        13020,
        13030,
        13040,
        13081,
        14010,
        14015,
        14020,
        14021,
        14022,
        14030,
        14031,
        14032,
        14040,
        14050,
        14081,
        15110,
        15111,
        15112,
        15113,
        15114,
        15125,
        15130,
        15142,
        15150,
        15151,
        15152,
        15153,
        15160,
        15170,
        15180,
        15190,
        15210,
        15220,
        15230,
        15240,
        15250,
        15261,
        16010,
        16020,
        16030,
        16040,
        16050,
        16061,
        16062,
        16063,
        16064,
        16070,
        16080,
        21010,
        21020,
        21030,
        21040,
        21050,
        21061,
        21081,
        22010,
        22020,
        22030,
        22040,
        23110,
        23181,
        23182,
        23183,
        23210,
        23220,
        23230,
        23231,
        23240,
        23260,
        23270,
        23330,
        23410,
        23630,
        23631,
        23640,
        23642,
        24010,
        24020,
        24030,
        24040,
        24050,
        24081,
        25010,
        25020,
        25030,
        25040,
        31110,
        31120,
        31130,
        31140,
        31150,
        31161,
        31162,
        31163,
        31164,
        31165,
        31166,
        31181,
        31182,
        31191,
        31192,
        31193,
        31194,
        31195,
        31210,
        31220,
        31261,
        31281,
        31282,
        31291,
        31310,
        31320,
        31381,
        31382,
        32110,
        32120,
        32130,
        32140,
        32161,
        32162,
        32163,
        32168,
        32169,
        32182,
        32210,
        32220,
        32262,
        32264,
        32265,
        32310,
        33110,
        33120,
        33130,
        33140,
        33150,
        33181,
        33210,
        41010,
        41020,
        41030,
        41040,
        41081,
        41082,
        43010,
        43030,
        43040,
        43050,
        43060,
        43071,
        43072,
        43073,
        43081,
        43082,
        51010,
        52010,
        53040,
        60010,
        60030,
        60040,
        72010,
        72040,
        72050,
        73010,
        74020,
        91010,
        93010,
        99810,
        99820,
    ]


def filter_covid_sectors(df: pd.DataFrame, health_only: bool = True) -> pd.DataFrame:
    # Load the list of sectors
    health = get_health_purpose_codes()
    covid = covid_sectors() if not health_only else []
    sectors = list(set(health + covid))

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
    return df.loc[
        lambda d: ~d.keywords.str.contains("covid", case=False, na=False)
    ].loc[lambda d: ~d.keywords.str.contains("c19", case=False, na=False)]


def keep_covid_keyword(df: pd.DataFrame) -> pd.DataFrame:
    #  any rows where the substring 'covid' appears in the keyword column
    return df.loc[
        lambda d: d.keywords.str.contains("covid", case=False, na=False)
        | d.keywords.str.contains("c19", case=False, na=False)
    ]


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


def flag_covid_keyword(df: pd.DataFrame) -> pd.DataFrame:
    # Any rows with 'covid' in the keyword column will have their purpose code remapped to 160
    df["covid_k"] = df.keywords.str.contains("covid", case=False, na=False)

    return df


def remap_covid_purpose(df: pd.DataFrame) -> pd.DataFrame:
    # Any rows with purpose code 12264 will have their purpose code remapped to 160
    df.loc[lambda d: d.purpose_code == 12264, "purpose_code"] = 160

    return df


def flag_covid_purpose(df: pd.DataFrame) -> pd.DataFrame:
    # Any rows with purpose code 12264 will have their purpose code remapped to 160
    df["covid_p"] = df.purpose_code == 12264

    return df


def remap_covid_trust_fund(df: pd.DataFrame) -> pd.DataFrame:
    # Any rows with donor code 1047 will have their donor code remapped to 160
    df.loc[lambda d: d.donor_code == 1047, "donor_code"] = 160

    return df


def flag_covid_trust_fund(df: pd.DataFrame) -> pd.DataFrame:
    # Any rows with donor code 1047 will have their donor code remapped to 160
    df["covid_t"] = df.donor_code == 1047

    return df


GROUPER = [
    "year",
    "indicator",
    "donor_code",
    "recipient_code",
    "project_title",
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
    df = oda.get_data().astype({"value": float}).pipe(filter_covid_sectors)

    # Group the data
    grouper = [c for c in GROUPER if c in df.columns]
    df = df.groupby(grouper, dropna=False, observed=True)["value"].sum().reset_index()

    return df
