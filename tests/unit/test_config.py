import pytest

from gaming_lakehouse.config import load_settings


def test_gcp_paths_are_gs():
    s = load_settings("gcp", "prod")
    assert s.layer_uri("bronze", "kaggle").startswith("gs://")
    assert "prod" in s.layer_uri("bronze")


def test_azure_paths_are_abfss():
    s = load_settings("azure", "qa")
    uri = s.layer_uri("silver", "events")
    assert uri.startswith("abfss://")
    assert ".dfs.core.windows.net" in uri


def test_environment_drives_quality_action():
    assert load_settings("gcp", "dev").on_violation == "warn"
    assert load_settings("gcp", "qa").on_violation == "drop"
    assert load_settings("gcp", "prod").on_violation == "fail"


def test_broker_switches_with_cloud():
    assert load_settings("gcp", "dev").broker == "pubsub"
    assert load_settings("azure", "dev").broker == "eventhubs"


def test_llm_provider_switches_with_cloud():
    assert load_settings("gcp", "dev").get("ai.llm_provider") == "vertex"
    assert load_settings("azure", "dev").get("ai.llm_provider") == "azure_openai"


@pytest.mark.parametrize("cloud,env", [("aws", "dev"), ("gcp", "staging")])
def test_invalid_toggles_are_rejected(cloud, env):
    with pytest.raises(ValueError):
        load_settings(cloud, env)


def test_table_names_are_three_level():
    s = load_settings("gcp", "prod")
    assert s.table("gold", "gold_player_360").count(".") == 2
