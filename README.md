# Brawlhalla — Legend & Weapon Performance Analysis

**Author:** Ohanzee Karneh
**Tools:** PySpark (data pipeline & visualization) | Jupyter Notebook
**Data Source:** https://brawlmance.com
---

## Project Overview

This project explores legend and weapon performance in Brawlhalla using two datasets from Brawlmance, which compiles ranked match statistics from the Brawlhalla developer API. The legends dataset covers 70 legends (base stats, weapons, play rate, win rate, damage, and match time), and the weapons dataset covers 15 weapons (play rate, win rate, damage dealt, match time, and time held). Both datasets were processed and visualized with PySpark to explore popularity versus performance, damage output, stat lines, and how weapon choice relates to legend success.

---

## Data Pipeline (PySpark)

1. **Data Ingestion** — Loaded `brawlmance-legends.csv` and `brawlmance-weapons.csv` into Spark DataFrames with `header=True` and `inferSchema=True`
2. **Ranking** — Used `orderBy(F.desc(...))` with `limit()` for top-N questions
3. **Derived Metrics** — Created the play rate × win rate product and the damage ratio with `withColumn`, rounded with `F.round`
4. **Weapon Slot Stacking** — Counted `weapon_one` and `weapon_two` separately, combined them with `unionByName`, and summed per weapon with `groupBy().agg(F.sum(...))`
5. **Stat Aggregation** — Summed each stat with `agg` and divided by the number of legends in the group
6. **Weapon Join** — Aliased the weapons table into two lookups (one per weapon slot) and left-joined them onto the legends table

---

## Questions & Findings

## Legends

### Q1A: What are the top 5 legends by play rate?

| Rank | Legend | Play Rate |
| --- | --- | --- |
| 1 | Mordex | 4.61% |
| 2 | Nix | 3.70% |
| 3 | Wu Shang | 3.70% |
| 4 | Diana | 3.21% |
| 5 | Teros | 2.96% |

**Key Finding:**

---

### Q1B: What are the top 5 legends by win rate?

| Rank | Legend | Win Rate |
| --- | --- | --- |
| 1 | Qinghua & Baobao | 52.26% |
| 2 | Magyar | 52.24% |
| 3 | Sir Roland | 52.17% |
| 4 | Thor | 52.12% |
| 5 | Imugi | 51.96% |

**Key Finding:**

---

### Q1C: Which legends have the highest play rate × win rate product?

**Methodology:** Multiplied each legend's play rate by its win rate to create a single combined measure of popularity and performance.

**Formula (PySpark):**

```python
legends_set = legends_set.withColumn("win_play_product", F.col("winrate") * F.col("playrate"))
```

| Rank | Legend | Win × Play Product |
| --- | --- | --- |
| 1 | | |
| 2 | | |
| 3 | | |
| 4 | | |
| 5 | | |

**Key Finding:**

---

### Q2A: Which legends deal the most damage per match?

| Rank | Legend | Damage Dealt |
| --- | --- | --- |
| 1 | Yumiko | 485.32 |
| 2 | Arcadia | 484.82 |
| 3 | Queen Nai | 482.49 |
| 4 | Imugi | 482.06 |
| 5 | Qinghua & Baobao | 478.92 |

**Key Finding:**

---

### Q2B: Which legends take the least damage per match?

| Rank | Legend | Damage Taken |
| --- | --- | --- |
| 1 | Thea | 396.29 |
| 2 | Rupture | 398.29 |
| 3 | Loki | 398.39 |
| 4 | King Zuva | 402.67 |
| 5 | Munin | 403.58 |

**Key Finding:**

---

### Q2C: Which legend has the best damage ratio?

**Methodology:** Divided each legend's damage dealt by its damage taken. A ratio above 1 means the legend deals more damage than it receives.

**Formula (PySpark):**

```python
legends_set = legends_set.withColumn("damage_ratio", F.round(F.col("damagedealt") / F.col("damagetaken"), 3))
```

| Rank | Legend | Damage Ratio |
| --- | --- | --- |
| 1 | Yumiko | 1.182 |
| 2 | Imugi | 1.177 |
| 3 | Queen Nai | 1.161 |
| 4 | Arcadia | 1.158 |
| 5 | Qinghua & Baobao | 1.152 |

**Key Finding:**

---

### Q3: Among the top 15 most-played legends, what is the most common weapon?

**Methodology:** Each legend has two weapon slots, so `weapon_one` and `weapon_two` were counted separately, stacked with `unionByName`, and summed per weapon.

| Weapon | Count |
| --- | --- |
| Fists | 5 |
| Spear | 5 |
| Sword | 4 |
| Bow | 3 |
| Pistol | 3 |
| Hammer | 3 |
| Scythe | 2 |
| Axe | 2 |
| Boots | 1 |
| Cannon | 1 |
| Rocket Lance | 1 |

**Key Finding:**

---

### Q4A & Q4B: What is the average stat line of a top 15 legend?

**Methodology:** Took the top 15 legends by play rate (Q4A) and by win rate (Q4B), summed each stat, and divided by 15.

| Group | Strength | Dexterity | Defense | Speed |
| --- | --- | --- | --- | --- |
| Top 15 by play rate | 5.40 | 5.33 | 5.27 | 6.00 |
| Top 15 by win rate | 5.93 | 4.47 | 6.87 | 4.73 |

**Key Finding:**

---

## Weapons

### Q5A & Q5B: What are the top 5 weapons by play rate and win rate?

| Rank | Weapon (Play Rate) | Play Rate | Weapon (Win Rate) | Win Rate |
| --- | --- | --- | --- | --- |
| 1 | Fists | 2.17% | Cannon | 51.27% |
| 2 | Pistol | 1.79% | Greatsword | 51.15% |
| 3 | Scythe | 1.77% | Chakram | 50.91% |
| 4 | Spear | 1.66% | Rocket Lance | 50.87% |
| 5 | Hammer | 1.50% | Hammer | 50.57% |

**Key Finding:**

---

### Q6A: Which weapons are tied to long matches?

| Rank | Weapon | Match Time (s) |
| --- | --- | --- |
| 1 | Scythe | 171.85 |
| 2 | Chakram | 171.42 |
| 3 | Pistol | 170.66 |
| 4 | Fists | 170.60 |
| 5 | Orb | 170.55 |

**Key Finding:**

---

### Q6B: Which weapon deals the most damage per match?

| Rank | Weapon | Damage Dealt |
| --- | --- | --- |
| 1 | Greatsword | 261.97 |
| 2 | Chakram | 244.63 |
| 3 | Cannon | 242.67 |
| 4 | Spear | 211.15 |
| 5 | Scythe | 210.42 |

**Key Finding:**

---

### Q7: What is the relationship between weapon choice and legend win rate?

**Methodology:** For the top 15 most-played legends, each legend's win rate was compared to the average win rate of its two weapons. A positive difference means the legend wins more than its weapons do on average.

**Formula (PySpark):**

```python
weapon_winrate = (
    weapon_winrate
    .withColumn("average_weapon_winrate",
                F.round((F.col("weapon_one_winrate") + F.col("weapon_two_winrate")) / 2, 2))
    .withColumn("winrate_diff",
                F.round(F.col("legend_winrate") - F.col("average_weapon_winrate"), 2))
)
```

| Legend | Legend Win Rate | Avg Weapon Win Rate | Difference |
| --- | --- | --- | --- |
| Sidra | 51.36 | 50.59 | +0.77 |
| Orion | 51.06 | 50.45 | +0.61 |
| Ada | 50.49 | 49.91 | +0.58 |
| Bödvar | 50.61 | 50.24 | +0.37 |
| Nix | 49.61 | 49.42 | +0.19 |
| Yumiko | 50.25 | 50.11 | +0.14 |
| Diana | 49.66 | 49.72 | -0.06 |
| Zariel | 49.38 | 49.47 | -0.09 |
| Val | 49.49 | 49.60 | -0.11 |
| Teros | 50.28 | 50.47 | -0.19 |
| Brynn | 49.95 | 50.19 | -0.24 |
| Tezca | 48.91 | 49.39 | -0.48 |
| Hattori | 49.30 | 49.97 | -0.67 |
| Wu Shang | 48.81 | 49.66 | -0.85 |
| Mordex | 48.19 | 49.17 | -0.98 |

**Key Finding:**

---

## Limitations

1. **Single snapshot** — Brawlmance updates daily, so results reflect one point in time and will shift on a newer pull.
2. **Play rate × win rate product** — A product is hard to interpret and lets play rate dominate. A z-score composite (standardizing both metrics across all 70 legends, then summing) would be a fairer combined measure.
3. **Win rate noise** — Legends with fewer games can show extreme win rates by chance. Weighting or shrinking by `games` would make rankings more robust.
4. **Aggregate weapon win rates** — Weapon win rates include every legend using that weapon, so the Q7 comparison is approximate rather than controlled.
5. **Descriptive only** — Findings show associations, not causes. Players pick strong legends, and popular legends generate more data.

---

## Tools & Technologies

| Tool | Purpose |
| --- | --- |
| **PySpark** | Data ingestion, transformation, joins, aggregation, and visualization |
| **Jupyter Notebook** | Development environment for the PySpark pipeline |

---

## Repository Contents

| File | Description |
| --- | --- |
| `Brawlhalla Analysis.ipynb` | PySpark analysis notebook |
| `brawlmance-legends.csv` | Raw legend statistics dataset |
| `brawlmance-weapons.csv` | Raw weapon statistics dataset |
| `README.md` | Project overview and documentation |
