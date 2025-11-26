from statusbrew_pipeline.config import Settings


def test_space_ids_parse():
    settings = Settings(
        gcp_project="proj",
        space_ids="a,b , c",
        statusbrew_access_token="token",
    )
    assert settings.space_ids == ["a", "b", "c"]
