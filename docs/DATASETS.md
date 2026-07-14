# Datasets

All ingested through the Kaggle API (`kaggle.api.dataset_download_files`), configured in
`conf/datasets.yaml`. Each lands under `<landing>/kaggle/<key>/ingest_date=YYYY-MM-DD/` and is
gated by a content checksum — an unchanged Kaggle version is not re-uploaded.

| Key | Kaggle slug | Covers | Feeds |
|---|---|---|---|
| `vgchartz_sales_classic` | `gregorut/videogamesales` | 16.5k+ titles, 1980–2016 — the community's default VGChartz extract | Physical sales backbone: 90s/00s/10s |
| `vgchartz_sales_extended` | `patkle/video-game-sales-data-from-vgchartzcom` | 60k+ titles with platform, shipped units, regional splits, critic scores | Long-tail titles, shipping data |
| `vgchartz_with_ratings` | `rush4ratio/video-game-sales-with-ratings` | Sales joined to Metacritic critic/user scores and ESRB ratings | Quality signal for the title mart |
| `gaming_profiles` | `artyomkruglov/gaming-profiles-2025-steam-playstation-xbox` | Player profiles, owned titles, prices, achievements across Steam/PS/Xbox | **Digital purchases** + player-level behaviour |
| `xbox_game_pass` | `deepcontractor/xbox-game-pass-games-library` | The full Game Pass catalog | **Membership** entitlement dimension |
| `playstation_catalog_ps4` | `shivamb/all-playstation-4-games` | ~10k PS4 titles with store metadata | PS catalog dimension |
| `playstation_catalog_ps5` | `kanchana1990/ps5-game-data-2000-titles-explored` | ~2k PS5 titles | Current-gen continuity |

## Why these

The brief covers four purchase modes — physical, digital, membership, and console hardware —
across three decades. No single public dataset covers all four, so they are composed:

* **Physical** and the historical eras come from the VGChartz family. It is the only public
  source with per-platform unit sales going back to the PS1 and the original Xbox.
* **Digital** and player behaviour come from the gaming-profiles dataset, which is the current
  community reference for cross-platform (Steam/PS/Xbox) player-level data.
* **Membership** comes from the Game Pass catalog plus the `subscriptions` OLTP table
  (PS Plus / Game Pass tiers, MRR, churn) that flows through CDC.
* **Console hardware** comes from the `consoles` OLTP table, seeded in `scripts/seed_oltp.sql`
  with the nine SKUs that anchor the PS and Xbox generations.

The OLTP tables are the *live* half of the picture: Kaggle gives history, Postgres+Debezium gives
the present. Silver joins them on the conformed platform and title keys.

## Licensing

Kaggle datasets carry their own licenses (mostly CC0 / CC BY-SA / ODbL). Check the individual
dataset page before any redistribution of the raw files. Derived aggregates in Gold are fine for
internal analytics; republishing the raw extracts is not automatically fine.
