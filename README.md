# Brawlhalla — Legend & Weapon Performance Analysis

**Author:** Ohanzee Karneh
<br><br>
**Data Source:** https://brawlmance.com
<br><br>
---

## Project Overview and Context

Brawlhalla is a free-to-play online fighting game created by Blue Mammoth Games in 2014 and released globally in 2017. The playable characters are called 'Legends', and each legend has a unique combination of weapons to fight with. They also have a unique stat line consisting of strength, dexterity, defense, and speed, which modify damage dealt, attack recovery time, damage taken, and movement speed, respectively. Each stat ranges from 1 to 10, and each character has 22 stat points total. Additionally, legends have signature moves for their weapons that are unavailable on other characters.

This project explores legend and weapon performance in Brawlhalla using two datasets from Brawlmance, which compiles ranked match statistics from the Brawlhalla developer API. The legends dataset covers all 70 legends currently available, detailing their base stats, weapons, play rate, win rate, damage, and match time. The weapons dataset covers all 15 weapons and their play rate, win rate, damage dealt, match time, and time held. Both datasets were processed and visualized with PySpark.

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

**Initial Prediction:** Mordex will be the most played legend.

**Key Finding:** While my prediction relied on anecdotal evidence from over 5 years of on-and-off play instead of empirical data, the results coincidentally lined up. However, I was surprised to see Diana in the top 5, considering that I rarely see her in lower-ranked play, where most of the player base is.

---

### Q1B: What are the top 5 legends by win rate?

| Rank | Legend | Win Rate |
| --- | --- | --- |
| 1 | Qinghua & Baobao | 52.26% |
| 2 | Magyar | 52.24% |
| 3 | Sir Roland | 52.17% |
| 4 | Thor | 52.12% |
| 5 | Imugi | 51.96% |

**Initial Prediction:** The top 5 legends by win rate will correlate with the most played legends.

**Key Finding:** None of the legends I expected made the top 5. Most of the legends that did had low play rates (1.8%, 1.31%, 0.65%, 0.53%, and 0.71% as of 11/10/26), so their players could be dedicated mains, which inflates their win rates.

---

### Q1C: Which legends have the highest play rate × win rate product?

| Rank | Legend | Win × Play Product |
| --- | --- | --- |
| 1 | Mordex | 222.16 |
| 2 | Nix | 183.56 |
| 3 | Wu Shang | 180.60 |
| 4 | Diana | 159.41 |
| 5 | Teros | 148.83 |
| 6 | Ada | 138.34 |
| 7 | Yumiko | 127.13 |
| 8 | Tezca | 126.68 |
| 9 | Sidra | 121.72 |
| 10 | Val | 109.37 |

**Key Finding:** Because playrate and winrate vary wildly in scale (playrate ranges from 0.5-5% while winrate hovers around 50% across the board), this metric tells little about the relationship between the two metrics. In future studies, ranking legends by their summed z-scores of both playrate and winrate may be a better approach.

---

### Q2A: Which legends deal the most damage per match?

| Rank | Legend | Damage Dealt |
| --- | --- | --- |
| 1 | Yumiko | 485.32 |
| 2 | Arcadia | 484.82 |
| 3 | Queen Nai | 482.49 |
| 4 | Imugi | 482.06 |
| 5 | Qinghua & Baobao | 478.92 |

**Initial Prediction:** High strength legends like Teros, Xull, and Scarlett will deal more damage per match because they hit harder on each attack.

**Key Finding:** The top damage dealers are split between legends with high strength (Arcadia, Queen Nai, Imugi) and legends with low strength (Yumiko, Qinqhua & Baobao). While the former's presence is self-explanatory, the latter's could be due to lacking knockout power. The strength stat is also tied to knockback, so weaker legends typically require more hits to push their opponents out of bounds.

---

### Q2B: Which legends take the least damage per match?

| Rank | Legend | Damage Taken |
| --- | --- | --- |
| 1 | Thea | 396.29 |
| 2 | Rupture | 398.29 |
| 3 | Loki | 398.39 |
| 4 | King Zuva | 402.67 |
| 5 | Munin | 403.58 |

**Initial Prediction:** High-defense legends that take less damage per hit, like Zariel, Onyx, and Magyar, will take the least damage overall.

**Key Finding:** The top 5 includes legends with high strength (Rupture, King Zuva), high dexterity (Thea, Loki), and one outlier (Munin). What unifies these characters is that they all have a play rate of less than 1%, so their high performance could again be attributed to highly skilled players. Alternatively, their players are knocked out early before taking too much damage.

---

### Q2C: Which legends have the best damage ratios (damage dealt vs damage taken)?

| Rank | Legend | Damage Ratio |
| --- | --- | --- |
| 1 | Yumiko | 1.182 |
| 2 | Imugi | 1.177 |
| 3 | Queen Nai | 1.161 |
| 4 | Arcadia | 1.158 |
| 5 | Qinghua & Baobao | 1.152 |

**Key Finding:** All of the legends in this list have excellent signature kits in addition either having high strength (Queen Nai, Arcadia, Imugi) or weapons that build up damage easily (Yumiko, Qinghua & Baobao).

---

### Q3: Among the top 15 most-played legends, what is the most common weapon?

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

**Key Finding:** Fists and Spear were the most common weapons found on the top 15 Legends. Both weapons (and sword that follows them) have simple strings and true combos, which could be attractive to a large subset of the playerbase.

---

### Q4A & Q4B: What is the average stat line of a top 15 legend?

| Group | Strength | Dexterity | Defense | Speed |
| --- | --- | --- | --- | --- |
| Top 15 by play rate | 5.40 | 5.33 | 5.27 | 6.00 |
| Top 15 by win rate | 5.93 | 4.47 | 6.87 | 4.73 |

**Key Finding:** Since most stat points hover around 5 for the play rate, a legend's stat line likely has little impact on popularity. Conversely, high win-rate legends have a skew towards strength and defense as primary stats.

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

**Key Finding:** The top weapons by play rate differ slightly from the most common weapons among the most-played legends. Weapon play rate is weighted by how often each legend is picked, so the top 5 legends pull the results toward their weapons. For example, the top two most played legends, Nix and Mordex, both have scythes, which increases the weapon's overall play rate. The top 5 by win rate, meanwhile, are weapons with high per-hit damage. I suspect these choices work better in low-skill lobbies.

---

### Q6A: Which weapons are tied to long matches?

| Rank | Weapon | Match Time (s) |
| --- | --- | --- |
| 1 | Scythe | 171.85 |
| 2 | Chakram | 171.42 |
| 3 | Pistol | 170.66 |
| 4 | Fists | 170.60 |
| 5 | Orb | 170.55 |

**Key Finding:** Most weapons in the list have unorthodox playstyles, which would lead to difficulties in an inexperienced players' hand and longer match times. Fists could be present in the top 5 due to several of the most played legends using it and therefore having more matches to inflate its average.

---

### Q6B: Which weapon deals the most damage per match?

| Rank | Weapon | Damage Dealt |
| --- | --- | --- |
| 1 | Greatsword | 261.97 |
| 2 | Chakram | 244.63 |
| 3 | Cannon | 242.67 |
| 4 | Spear | 211.15 |
| 5 | Scythe | 210.42 |

**Key Finding:** Since the weapons in this list are almost identical to those with the highest win rates, a high damage-per-hit also explains the rankings. Scythe is the outlier. It relies on escapable combo strings, so it has fewer ways to get an early knockout and may end up dealing more damage before it does.

---

### Q7: What is the relationship between weapon choice and legend win rate?

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

**Key Finding:** I was surprised to see Sidra, Orion, and Ada performing as well as they did. They are popular legends and I had anticipated that it would bring their winrate differential down, but it could be that their overall abilities (weapon combination, stats, signature moves, etc.) overcame that problem. For Mordex, Wu Shang, and Teros, however, their popularity did put them in the negatives despite having lower weapon winrates to begin with.

---

## Limitations

1. **Single snapshot** — Brawlmance updates daily, so results reflect one point in time and will shift on a newer pull.
2. **Does not include ranked match data** - The brawlhalla API does not include game history for any ranked seasons, so that data does not influence these results.
3. **Play rate × win rate product** — A product is hard to interpret and lets play rate dominate. A z-score composite (standardizing both metrics across all 70 legends, then summing) would be a fairer combined measure.
4. **Win rate noise** — Legends with fewer games can show extreme win rates by chance. Weighting or shrinking by `games` would make rankings more robust.
5. **Aggregate weapon win rates** — Weapon win rates include every legend using that weapon, so the Q7 comparison is approximate rather than controlled.
6. **Descriptive only** — Findings show associations, not causes. Players pick strong legends, and popular legends generate more data.

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
| `images` | Output screenshots |
