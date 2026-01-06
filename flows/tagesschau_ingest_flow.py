from prefect import flow
from src.tagesschau_client import TagesschauClient


@flow(name="tagesschau_ingest")
def tagesschau_ingest():
    client = TagesschauClient(
        api_config_path="Docker_Container/config/api_config.json",
        regions_path="Docker_Container/config/regions.json",
        source_regions_path="Docker_Container/config/source_regions.json",
        url_region_keywords_path="Docker_Container/config/url_region_keywords.json",
        filters_path="Docker_Container/config/filters.json",
        db_path="Docker_Container/output/news_data/master.db",
    )

    client.collect_and_store()


if __name__ == "__main__":
    tagesschau_ingest()


