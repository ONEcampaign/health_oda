import pandas as pd
from oda_data import donor_groupings

from scripts import config
from scripts.bilateral import get_bilateral_health_oda
from scripts.imputed_multilateral import get_imputed_multilateral_health_oda

DONORS = [
    # ([4, 5, 6, 7, 918], "EUR"),
    # ([302], "USD"),
    # ([301], "CAD"),
    # ([12], "GBP"),
    # (918)
]


def export_total_bi_plus_multi_health_spending(
    donors: list[int] = None,
    start_year: int = 2012,
    end_year: int = 2022,
    currency: str = "USD",
    prices: str = "constant",
    base_year: int | None = 2022,
    export_by_donor: bool = False,
) -> None:

    donors = donors or [d for d, c in DONORS]

    bi_covid = get_bilateral_health_oda(
        start_year=start_year,
        end_year=end_year,
        prices=prices,
        currency=currency,
        base_year=base_year,
        exclude_covid=False,
        additional_groupers=["project_title", "purpose_code"],
    )
    bi = get_bilateral_health_oda(
        start_year=start_year,
        end_year=end_year,
        prices=prices,
        currency=currency,
        base_year=base_year,
        exclude_covid=True,
        additional_groupers=["project_title", "purpose_code"],
    )
    multi_covid = get_imputed_multilateral_health_oda(
        start_year=start_year,
        end_year=end_year,
        prices=prices,
        currency=currency,
        base_year=base_year,
        exclude_covid=False,
    )
    multi = get_imputed_multilateral_health_oda(
        start_year=start_year,
        end_year=end_year,
        prices=prices,
        currency=currency,
        base_year=base_year,
        exclude_covid=True,
    )

    bilateral = pd.concat(
        [
            bi.assign(indicator="Health ODA"),
            bi_covid.assign(indicator="Health ODA (including COVID-19)"),
        ],
        ignore_index=True,
    ).loc[lambda d: d.donor_code.isin(donors)]

    multilateral = pd.concat(
        [
            multi.assign(indicator="Health ODA"),
            multi_covid.assign(indicator="Health ODA (including COVID-19)"),
        ],
        ignore_index=True,
    ).loc[lambda d: d.donor_code.isin(donors)]

    # Combine the data
    data = pd.concat([bilateral, multilateral], ignore_index=True)

    # Summarize the data
    data = (
        data.groupby(
            ["year", "donor_code", "prices", "indicator"], observed=True, dropna=False
        )["value"]
        .sum()
        .reset_index()
    )

    # Reshape for export
    data = data.pivot(
        index=["year", "donor_code", "prices"], columns="indicator", values="value"
    ).reset_index()

    # Add donor names
    data["donor_name"] = data.donor_code.map(donor_groupings()["dac_members"])

    # Clean the dataframe
    data = data.filter(
        [
            "year",
            "donor_name",
            "Health ODA (including COVID-19)",
            "Health ODA",
        ]
    )

    # Export the data
    if export_by_donor:
        for donor in data.donor_name.unique():
            donor_data = data.loc[lambda d: d.donor_name == donor]
            donor_data.to_csv(
                config.Paths.output / f"{donor}_total_health_{prices}_{currency}.csv",
                index=False,
            )

    else:
        data.to_csv(
            config.Paths.output / "bi_plus_multi_health_spending_multiple_donors.csv",
            index=False,
        )


if __name__ == "__main__":

    export_total_bi_plus_multi_health_spending(
        donors=[4, 5, 6, 7, 918, 301, 302, 12],
        start_year=2019,
        end_year=2023,
        currency="USD",
        prices="constant",
        base_year=2023,
        export_by_donor=False,
    )
