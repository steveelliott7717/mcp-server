#!/usr/bin/env python3
"""
Generate synthetic seed data for the dbt project.

Every column name and type here mirrors the real DDL in ../../supabase/schema/.
The SHAPE is real; the ROWS are synthetic. Nothing in this file touches a live
database — it writes CSVs and nothing else.

Deterministic: seeded RNG, so `python generate_seeds.py` reproduces the same
CSVs byte for byte. Regenerate with a different --seed to get a fresh dataset.
"""
from __future__ import annotations

import argparse
import csv
import random
from datetime import date, timedelta
from pathlib import Path

HERE = Path(__file__).parent

START = date(2026, 7, 1)
END = date(2026, 8, 31)
LB_TO_KG = 0.453592


def daterange(a: date, b: date):
    d = a
    while d <= b:
        yield d
        d += timedelta(days=1)


def write(name: str, header: list[str], rows: list[list]) -> None:
    path = HERE / f"{name}.csv"
    with path.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)
    print(f"  {name}.csv  ({len(rows)} rows)")


def main(seed: int) -> None:
    rng = random.Random(seed)
    days = list(daterange(START, END))

    # ---- professional_profile.user_profile ---------------------------------
    write(
        "profile__user_profile",
        ["id", "first_name", "last_name", "date_of_birth", "sex", "height_in"],
        [[1, "Alex", "Rivera", "1996-04-12", "male", 70]],
    )

    # ---- health.weight_logs ------------------------------------------------
    # Slow gain with daily noise; a few missed weigh-ins so the rolling
    # averages have real gaps to cope with.
    weights: dict[date, float] = {}
    base = 178.0
    w_rows = []
    wid = 1
    for i, d in enumerate(days):
        if rng.random() < 0.10:      # ~10% missed weigh-ins
            continue
        base += 0.035 + rng.gauss(0, 0.45)
        val = round(base, 1)
        weights[d] = val
        w_rows.append([wid, val, d.isoformat()])
        wid += 1
    write("health__weight_logs", ["id", "weight_lbs", "log_date"], w_rows)

    # Rolling 3-day average, used below to price step/strength calories the
    # same way the production triggers do.
    def avg3(d: date) -> float:
        vals = [weights[d - timedelta(days=k)] for k in range(3) if d - timedelta(days=k) in weights]
        return sum(vals) / len(vals) if vals else 178.0

    # ---- health.step_logs --------------------------------------------------
    s_rows = []
    for i, d in enumerate(days, start=1):
        steps = rng.randint(6000, 14000)
        # actual_steps_taken arrives on a 2-day lag; the last two days are NULL
        if d <= END - timedelta(days=2):
            actual = max(0, int(steps * rng.uniform(0.85, 1.12)))
            kcal = round(actual * avg3(d) * LB_TO_KG * 0.00055, 1)
        else:
            actual, kcal = "", 0
        s_rows.append([i, d.isoformat(), steps, steps, actual, kcal])
    write(
        "health__step_logs",
        ["id", "date", "steps", "steps_agg", "actual_steps_taken", "calories_burned"],
        s_rows,
    )

    # ---- health.workout_logs ----------------------------------------------
    lifts = [
        ("Back Squat", 1, "Legs"), ("Bench Press", 2, "Push"),
        ("Deadlift", 3, "Hinge"), ("Overhead Press", 4, "Push"),
        ("Barbell Row", 5, "Pull"), ("Pull-Up", 6, "Pull"),
    ]
    wo_rows, woid = [], 1
    for d in days:
        if d.weekday() not in (0, 1, 3, 4):     # 4 sessions/week
            continue
        session = rng.sample(lifts, 3)
        for order, (nm, ex_id, cat) in enumerate(session, start=1):
            sets = rng.randint(3, 5)
            reps = rng.randint(5, 10)
            wt = rng.choice([95, 115, 135, 155, 185, 205, 225])
            rpe = round(rng.uniform(6.0, 9.5), 1)
            volume = sets * reps * wt
            eff = 0.07 + (min(max(rpe, 0), 10) / 10.0) ** 1.6 * 0.04
            kcal = round(volume * eff * 0.01, 1)
            wo_rows.append([woid, f"{cat} Day", ex_id, d.isoformat(), order, cat,
                            sets, reps, wt, rpe, volume, kcal])
            woid += 1
    write(
        "health__workout_logs",
        ["id", "workout_name", "exercise_id", "scheduled_date", "exercise_order",
         "category", "sets_completed", "reps", "weight_lbs", "rpe",
         "total_volume", "calories_burned"],
        wo_rows,
    )

    # ---- health.bike_logs  (RECONSTRUCTED — no DDL in repo) ----------------
    b_rows, bid = [], 1
    for d in days:
        if d.weekday() not in (2, 5):
            continue
        mins = rng.randint(25, 70)
        b_rows.append([bid, d.isoformat(), mins, round(mins * rng.uniform(7.5, 11.0), 1)])
        bid += 1
    write("health__bike_logs",
          ["id", "scheduled_date", "duration_minutes", "calories_burned"], b_rows)

    # ---- health.weekly_programs  (RECONSTRUCTED — no DDL in repo) ----------
    wp_rows, wpid, cur = [], 1, START
    while cur <= END:
        wp_rows.append([wpid, cur.isoformat(), rng.choice([-500, -250, 0, 250, 400])])
        wpid += 1
        cur += timedelta(days=14)
    write("health__weekly_programs", ["id", "start_date", "kcal_adjustment"], wp_rows)

    # ---- health.food_items -------------------------------------------------
    foods = [
        (1, "Chicken Breast", "Kirkland", "protein", 100, "g", 165, 31.0, 3.6, 0.0),
        (2, "White Rice", "Nishiki", "grain", 100, "g", 130, 2.7, 0.3, 28.2),
        (3, "Olive Oil", "Bertolli", "fat", 15, "ml", 119, 0.0, 13.5, 0.0),
        (4, "Broccoli", None, "vegetable", 100, "g", 34, 2.8, 0.4, 6.6),
        (5, "Greek Yogurt", "Fage", "dairy", 170, "g", 100, 18.0, 0.7, 6.0),
        (6, "Rolled Oats", "Quaker", "grain", 40, "g", 150, 5.0, 3.0, 27.0),
        (7, "Whey Protein", "Optimum", "supplement", 30, "g", 120, 24.0, 1.5, 3.0),
        (8, "Banana", None, "fruit", 118, "g", 105, 1.3, 0.4, 27.0),
        (9, "Ground Beef 85/15", None, "protein", 100, "g", 250, 26.0, 15.0, 0.0),
        (10, "Sweet Potato", None, "vegetable", 130, "g", 112, 2.0, 0.1, 26.0),
    ]
    write(
        "health__food_items",
        ["id", "name", "brand", "category", "serving_amount", "serving_unit",
         "calories", "protein", "fat", "carbs"],
        [[i, n, b or "", c, sa, su, kc, p, f, cb] for i, n, b, c, sa, su, kc, p, f, cb in foods],
    )

    # ---- health.recipes + recipe_ingredients -------------------------------
    # Recipe macros are the SUM of scaled ingredients — the production DB keeps
    # this true with the update_recipe_nutrition trigger; here it is computed,
    # and schema.yml asserts it stays true.
    recipes = [
        (1, "Chicken and Rice", "main", [(1, 200), (2, 150), (3, 15)]),
        (2, "Protein Oats",     "breakfast", [(6, 80), (7, 30), (8, 118)]),
        (3, "Beef Bowl",        "main", [(9, 200), (10, 260), (4, 100)]),
    ]
    food_by_id = {f[0]: f for f in foods}
    r_rows, ri_rows, riid = [], [], 1
    for rid, name, cat, ings in recipes:
        tot = [0.0, 0.0, 0.0, 0.0]
        for fid, grams in ings:
            f = food_by_id[fid]
            scale = grams / f[4]
            tot[0] += f[6] * scale
            tot[1] += f[7] * scale
            tot[2] += f[8] * scale
            tot[3] += f[9] * scale
            ri_rows.append([riid, rid, fid, grams, "g"])
            riid += 1
        r_rows.append([rid, name, cat, round(tot[0], 1), round(tot[1], 1),
                       round(tot[2], 1), round(tot[3], 1)])
    write("health__recipes",
          ["id", "name", "category", "total_calories", "protein_g", "fat_g", "carbs_g"], r_rows)
    write("health__recipe_ingredients",
          ["id", "recipe_id", "food_item_id", "quantity", "unit"], ri_rows)

    # ---- health.meal_logs --------------------------------------------------
    # Macros are pre-scaled by quantity, exactly as f_fill_meal_macros does.
    recipe_by_id = {r[0]: r for r in r_rows}
    m_rows, mid = [], 1
    for d in days:
        for _ in range(rng.randint(3, 5)):
            if rng.random() < 0.7:
                rid = rng.choice([1, 2, 3])
                q = round(rng.uniform(0.75, 1.5), 2)
                _, _, _, kc, p, f, cb = recipe_by_id[rid]
                m_rows.append([mid, rid, "", q, round(kc * q, 1), round(p * q, 1),
                               round(f * q, 1), round(cb * q, 1), d.isoformat()])
            else:
                fid = rng.choice(list(food_by_id))
                fo = food_by_id[fid]
                q = round(rng.uniform(0.5, 2.0), 2)
                m_rows.append([mid, "", fid, q, round(fo[6] * q, 1), round(fo[7] * q, 1),
                               round(fo[8] * q, 1), round(fo[9] * q, 1), d.isoformat()])
            mid += 1
    write(
        "health__meal_logs",
        ["id", "recipe_id", "food_item_id", "quantity", "calories",
         "protein_g", "fat_g", "carbs_g", "logged_date"],
        m_rows,
    )

    # ---- finance.purchases -------------------------------------------------
    vendors = [("Amazon", "household"), ("REWE", "groceries"), ("MediaMarkt", "electronics"),
               ("dm", "household"), ("Edeka", "groceries"), ("Decathlon", "sport")]
    items = ["Protein Powder", "Cleaning Spray", "USB-C Cable", "Shampoo",
             "Chicken Breast 2kg", "Resistance Bands", "Coffee Beans", "Notebook"]
    p_rows = []
    for pid in range(1, 46):
        v, cat = rng.choice(vendors)
        d = rng.choice(days)
        base_cost = round(rng.uniform(4, 120), 2)
        p_rows.append([
            pid, cat, v, rng.choice(items),
            rng.choice([1, 2, 3]),
            base_cost,
            round(rng.choice([0, 0, 0, 3.99, 5.95]), 2),
            round(base_cost * 0.19, 2),
            0.0,
            d.isoformat(),
            rng.choice(["Visa", "Mastercard"]),
            rng.choice(["4291", "7733", "1042"]),
        ])
    write(
        "finance__purchases",
        ["id", "category", "vendor", "item_name", "quantity", "base_cost",
         "shipping_cost", "tax_cost", "other_cost", "purchase_date",
         "card_type", "card_last4"],
        p_rows,
    )

    # ---- finance.recurring_purchases + charges -----------------------------
    subs = [
        (1, "software", "GitHub", "Copilot Pro", 10.00, "monthly"),
        (2, "software", "Anthropic", "Claude Pro", 20.00, "monthly"),
        (3, "media", "Spotify", "Premium Duo", 14.99, "monthly"),
        (4, "utilities", "Vodafone", "Mobile Plan", 29.99, "monthly"),
        (5, "fitness", "McFit", "Gym Membership", 24.90, "monthly"),
        (6, "software", "JetBrains", "All Products Pack", 289.00, "yearly"),
    ]
    rp_rows, rc_rows, rcid = [], [], 1
    for sid, cat, vendor, item, cost, freq in subs:
        start = START - timedelta(days=rng.randint(30, 300))
        rp_rows.append([sid, cat, vendor, item, 1, cost, 0.0, round(cost * 0.19, 2), 0.0,
                        freq, start.isoformat(),
                        (END + timedelta(days=rng.randint(1, 28))).isoformat(),
                        "true" if sid != 5 else "false",
                        rng.choice(["Visa", "Mastercard"]), rng.choice(["4291", "7733"])])
        step = 365 if freq == "yearly" else 30
        d = START
        while d <= END:
            rc_rows.append([rcid, sid, vendor, item, d.isoformat(), d.isoformat(),
                            cost, round(cost * 0.19, 2), 0.0, "false"])
            rcid += 1
            d += timedelta(days=step)
    write(
        "finance__recurring_purchases",
        ["id", "category", "vendor", "item_name", "quantity", "base_cost",
         "shipping_cost", "tax_cost", "other_cost", "frequency",
         "start_charge_date", "next_charge_date", "active", "card_type", "card_last4"],
        rp_rows,
    )
    write(
        "finance__recurring_purchase_charges",
        ["id", "recurring_purchase_id", "vendor", "item_name", "charge_date",
         "invoice_date", "base_cost", "tax_cost", "other_cost", "manually_overridden"],
        rc_rows,
    )


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=20260906)
    args = ap.parse_args()
    print(f"Generating synthetic seeds (seed={args.seed}) …")
    main(args.seed)
    print("Done. These are SYNTHETIC rows against the REAL schema shape.")
