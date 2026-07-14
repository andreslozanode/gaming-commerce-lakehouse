from gaming_lakehouse.ingestion.reference_dims import build_dim_console, build_dim_purchase_channel


def test_console_families_cover_ps_and_xbox():
    df = build_dim_console()
    families = set(df["console_family"].astype(str))
    assert families == {"PlayStation", "Xbox"}


def test_eras_are_binned_correctly():
    df = build_dim_console().set_index("platform_code")
    assert str(df.loc["PS", "era"]) == "90s"  # PS1, 1994
    assert str(df.loc["PS2", "era"]) == "00s"  # 2000
    assert str(df.loc["PS4", "era"]) == "10s"  # 2013
    assert str(df.loc["PS5", "era"]) == "20s"  # 2020


def test_channel_types_cover_all_purchase_modes():
    df = build_dim_purchase_channel()
    assert set(df["channel_type"].astype(str)) == {"PHYSICAL", "DIGITAL", "MEMBERSHIP", "HARDWARE"}
