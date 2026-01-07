from app.db import ensure_schema, fetch_tournaments, insert_tournament
from app.settings import load_settings


def ensure_base_tournament() -> None:
    settings = load_settings()
    ensure_schema(settings.database_url)
    tournaments = fetch_tournaments(settings.database_url)
    existing = {t["name"] for t in tournaments}
    default_name = "GSPro League"
    if default_name in existing:
        print(f"Tournament '{default_name}' already exists.")
        return
    insert_tournament(
        settings.database_url,
        name=default_name,
        description="Foundation event required before creating matches.",
    )
    print(f"Created tournament '{default_name}'.")


if __name__ == "__main__":
    ensure_base_tournament()
