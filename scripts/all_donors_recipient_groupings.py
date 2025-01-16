import pandas as pd
from bblocks import convert_id

from scripts import config
from scripts.bilateral import get_bilateral_health_oda
from scripts.common import add_income_grouping


def groupby_excluding(df: pd.DataFrame, exclude: list[str]) -> pd.DataFrame:
    return (
        df.groupby(
            [c for c in df.columns if c not in exclude + ["value"]],
            dropna=False,
            observed=True,
        )["value"]
        .sum()
        .reset_index()
    )


def africa_not_africa(df: pd.DataFrame) -> pd.DataFrame:
    df["continent"] = convert_id(
        df.recipient_code,
        from_type="DACCode",
        to_type="continent",
        not_found="",
        additional_mapping={
            189: "Africa",
            289: "Africa",
            298: "Africa",
            270: "Africa",
            1027: "Africa",
            1028: "Africa",
            1029: "Africa",
            1030: "Africa",
        },
    )
    africa = (
        df.loc[lambda d: d.continent == "Africa"]
        .pipe(groupby_excluding, exclude=["continent", "recipient_code"])
        .assign(recipient="Africa")
    )
    not_africa = (
        df.loc[lambda d: d.continent != "Africa"]
        .pipe(groupby_excluding, exclude=["continent", "recipient_code"])
        .assign(recipient="Other regions")
    )

    return pd.concat([africa, not_africa], ignore_index=True)


def by_regions(df: pd.DataFrame) -> pd.DataFrame:
    df["recipient"] = convert_id(
        df.recipient_code,
        from_type="DACCode",
        to_type="continent",
        not_found="Other",
        additional_mapping={
            189: "Africa",
            289: "Africa",
            298: "Africa",
            270: "Africa",
            1027: "Africa",
            1028: "Africa",
            1029: "Africa",
            1030: "Africa",
            89: "Europe",
            389: "America",
            489: "America",
            498: "America",
            1031: "America",
            1032: "America",
            589: "Asia",
            619: "Asia",
            679: "Asia",
            689: "Asia",
            789: "Asia",
            798: "Asia",
            889: "Oceania",
            1033: "Oceania",
            1034: "Oceania",
            1035: "Oceania",
        },
    )
    data = df.pipe(groupby_excluding, exclude=["recipient_code"])

    return data


def low_income_other_income(df: pd.DataFrame) -> pd.DataFrame:
    df = add_income_grouping(df)

    low_income = (
        df.loc[lambda d: d.income_level == "Low income"]
        .pipe(groupby_excluding, exclude=["income_level", "recipient_code"])
        .assign(recipient="Low income")
    )

    other_income = (
        df.loc[lambda d: d.income_level != "Low income"]
        .reset_index(drop=True)
        .pipe(groupby_excluding, exclude=["income_level", "recipient_code"])
        .assign(recipient="Other income levels")
    )

    return pd.concat([low_income, other_income], ignore_index=True)


def by_income(df: pd.DataFrame) -> pd.DataFrame:
    df = add_income_grouping(df).rename(columns={"income_level": "recipient"})
    df.recipient = df.recipient.fillna("Not classified by income level")
    data = df.pipe(groupby_excluding, exclude=["recipient_code"])

    return data


def health_with_and_without_covid(
    prices: str = "constant",
    base_year: int = 2023,
    start_year: int = 2015,
    end_year: int = 2023,
) -> pd.DataFrame:

    grouper = ["year", "recipient_code"]

    # Get the data for health
    health = (
        get_bilateral_health_oda(
            start_year=start_year,
            end_year=end_year,
            prices=prices,
            base_year=base_year,
            exclude_covid=False,
            by_recipient=True,
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
            by_recipient=True,
        )
        .groupby(grouper, observed=True, dropna=False)["value"]
        .sum()
        .reset_index()
        .assign(indicator="Health ODA")
    )

    data = pd.concat([health, health_without_covid], ignore_index=True)

    regions = africa_not_africa(data.copy())
    income_levels = low_income_other_income(data.copy())

    data = pd.concat([regions, income_levels], ignore_index=True)

    data = data.pivot(
        index=["year", "recipient"], columns="indicator", values="value"
    ).reset_index()

    return data


if __name__ == "__main__":
    df = health_with_and_without_covid(start_year=2008)
    df.to_csv(
        config.Paths.output / "health_by_recipient_income_constant.csv", index=False
    )
