import asyncio
from src.tagesschau_client import TagesschauClient


async def main():
    client = TagesschauClient(
        api_config_path="config/api_config.json",
        regions_path="config/regions.json",
        source_regions_path="config/source_regions.json",
        url_region_keywords_path="config/url_region_keywords.json",
        filters_path="config/filters.json",
    )

    await client.collect_and_store()


if __name__ == "__main__":
    asyncio.run(main())

