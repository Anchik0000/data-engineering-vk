import csv
import io
import itertools
import json
import time
import tracemalloc
from collections import defaultdict

import boto3
import duckdb

S3_ENDPOINT = "http://localhost:9000"
S3_ACCESS_KEY = "hhadmin"
S3_SECRET_KEY = "hhadmin-secret"
BUCKET_NAME = "hh-lake"
FILE_KEY = "it_vacancies_2024.csv"

s3_client = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
)


def run_mapreduce():
    print("=== Запуск MapReduce ===")
    tracemalloc.start()
    start_time = time.time()

    mapped_data = defaultdict(int)

    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=FILE_KEY)
    text_stream = io.TextIOWrapper(obj["Body"], encoding="utf-8")
    reader = csv.DictReader(text_stream)

    for row in reader:
        role = row.get("role", "")
        name = row.get("name", "")

        if "ПРОГРАММИСТ" in role.upper() and "1С" in name.upper():
            skills_str = row.get("key_skills", "")
            if not skills_str:
                continue

            skills = sorted([s.strip() for s in skills_str.split(";") if s.strip()])

            for s in skills:
                mapped_data[s] += 1
            for pair in itertools.combinations(skills, 2):
                mapped_data[f"{pair[0]} + {pair[1]}"] += 1
            for triplet in itertools.combinations(skills, 3):
                mapped_data[f"{triplet[0]} + {triplet[1]} + {triplet[2]}"] += 1

    sorted_skills = sorted(mapped_data.items(), key=lambda x: x[1], reverse=True)

    exec_time = time.time() - start_time
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    print(f"MapReduce завершен за {exec_time:.2f} сек.")
    print(f"Пиковое потребление памяти: {peak / 10**6:.2f} MB")

    result_json = json.dumps(sorted_skills[:500], ensure_ascii=False)
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key="mapreduce_1c_results.json",
        Body=result_json.encode("utf-8"),
    )
    print("-> Результаты MapReduce сохранены в S3 (mapreduce_1c_results.json)")

    single_skills = [(k, v) for k, v in sorted_skills if " + " not in k]
    return single_skills[:5]


def run_duckdb():
    print("\n=== Запуск DuckDB ===")
    tracemalloc.start()
    start_time = time.time()

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(
        f"CREATE SECRET s3 (TYPE S3, KEY_ID '{S3_ACCESS_KEY}', SECRET '{S3_SECRET_KEY}', REGION 'us-east-1', ENDPOINT 'localhost:9000', USE_SSL false, URL_STYLE 'path');"
    )

    con.execute(f"""
        CREATE TEMP TABLE v1c_skills AS 
        SELECT trim(skill) as skill
        FROM (
            SELECT string_split(key_skills, ';') as skills_arr 
            FROM read_csv_auto('s3://{BUCKET_NAME}/{FILE_KEY}', ignore_errors=true)
            WHERE upper(role) LIKE '%ПРОГРАММИСТ%' AND upper(name) LIKE '%1С%' AND key_skills IS NOT NULL
        ), UNNEST(skills_arr) as t(skill);
    """)

    top_single = con.execute("""
        SELECT skill, count(*) as cnt 
        FROM v1c_skills 
        WHERE skill != '' 
        GROUP BY skill 
        ORDER BY cnt DESC 
        LIMIT 5;
    """).fetchall()

    exec_time = time.time() - start_time
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    print(f"DuckDB завершен за {exec_time:.2f} сек.")
    print(f"Пиковое потребление памяти: {peak / 10**6:.2f} MB")

    return top_single


def analyze_salaries():
    print("\n" + "=" * 50)
    print("=== АНАЛИЗ ВЛИЯНИЯ НАВЫКОВ НА ЗАРПЛАТУ (1С) ===")
    print("=" * 50)

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(
        f"CREATE SECRET s3 (TYPE S3, KEY_ID '{S3_ACCESS_KEY}', SECRET '{S3_SECRET_KEY}', REGION 'us-east-1', ENDPOINT 'localhost:9000', USE_SSL false, URL_STYLE 'path');"
    )

    base_salary = con.execute(f"""
        SELECT median(salary_from) FROM read_csv_auto('s3://{BUCKET_NAME}/{FILE_KEY}', ignore_errors=true)
        WHERE upper(role) LIKE '%ПРОГРАММИСТ%' AND upper(name) LIKE '%1С%' AND salary_from IS NOT NULL;
    """).fetchone()[0]
    print(f"\n-> БАЗОВАЯ МЕДИАННАЯ ЗАРПЛАТА 1С-РАЗРАБОТЧИКА: {base_salary:,.0f} руб.\n")

    rows = con.execute(f"""
        SELECT key_skills, salary_from 
        FROM read_csv_auto('s3://{BUCKET_NAME}/{FILE_KEY}', ignore_errors=true)
        WHERE upper(role) LIKE '%ПРОГРАММИСТ%' AND upper(name) LIKE '%1С%' 
          AND salary_from IS NOT NULL AND key_skills IS NOT NULL;
    """).fetchall()

    pairs_salaries = defaultdict(list)
    triplets_salaries = defaultdict(list)

    for skills_str, sal in rows:
        skills = sorted([s.strip() for s in skills_str.split(";") if s.strip()])
        for p in itertools.combinations(skills, 2):
            pairs_salaries[f"{p[0]} + {p[1]}"].append(sal)
        for t in itertools.combinations(skills, 3):
            triplets_salaries[f"{t[0]} + {t[1]} + {t[2]}"].append(sal)

    import statistics

    def get_stats(data_dict, min_count=5):
        stats = []
        for combo, sals in data_dict.items():
            if len(sals) >= min_count:
                med = statistics.median(sals)
                diff = med - base_salary
                stats.append((combo, len(sals), med, diff))
        return stats

    pairs_stats = get_stats(pairs_salaries, min_count=5)
    triplets_stats = get_stats(triplets_salaries, min_count=5)

    pairs_up = sorted(pairs_stats, key=lambda x: x[2], reverse=True)[:3]
    pairs_down = sorted(pairs_stats, key=lambda x: x[2])[:3]

    triplets_up = sorted(triplets_stats, key=lambda x: x[2], reverse=True)[:3]
    triplets_down = sorted(triplets_stats, key=lambda x: x[2])[:3]

    print(">>> ТОП-3 ПАР НАВЫКОВ, ПОВЫШАЮЩИХ ЗАРПЛАТУ:")
    for combo, cnt, med, diff in pairs_up:
        print(
            f"  [+{diff:,.0f} руб.] {combo} (Медиана: {med:,.0f} руб., Вакансий: {cnt})"
        )

    print("\n>>> ТОП-3 ПАР НАВЫКОВ, ПОНИЖАЮЩИХ ЗАРПЛАТУ:")
    for combo, cnt, med, diff in pairs_down:
        print(
            f"  [{diff:,.0f} руб.] {combo} (Медиана: {med:,.0f} руб., Вакансий: {cnt})"
        )

    print("\n" + "-" * 50)
    print(">>> ТОП-3 ТРОЕК НАВЫКОВ, ПОВЫШАЮЩИХ ЗАРПЛАТУ:")
    for combo, cnt, med, diff in triplets_up:
        print(
            f"  [+{diff:,.0f} руб.] {combo} (Медиана: {med:,.0f} руб., Вакансий: {cnt})"
        )

    print("\n>>> ТОП-3 ТРОЕК НАВЫКОВ, ПОНИЖАЮЩИХ ЗАРПЛАТУ:")
    for combo, cnt, med, diff in triplets_down:
        print(
            f"  [{diff:,.0f} руб.] {combo} (Медиана: {med:,.0f} руб., Вакансий: {cnt})"
        )
    print("=" * 50 + "\n")


def analyze_regions():
    print("\n" + "=" * 50)
    print("=== РЕГИОНАЛЬНЫЙ АНАЛИЗ: МОСКВА vs САНКТ-ПЕТЕРБУРГ vs ЯКУТСК ===")
    print("=" * 50)

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(
        f"CREATE SECRET s3 (TYPE S3, KEY_ID '{S3_ACCESS_KEY}', SECRET '{S3_SECRET_KEY}', REGION 'us-east-1', ENDPOINT 'localhost:9000', USE_SSL false, URL_STYLE 'path');"
    )

    regions = ["Москва", "Санкт-Петербург", "Якутск"]

    df_reg = con.execute(f"""
        SELECT 
            area as "Регион", 
            count(*) as "Всего вакансий", 
            count(salary_from) as "Вакансий с указанной ЗП",
            round(median(salary_from), 0) as "Медианная ЗП (руб.)"
        FROM read_csv_auto('s3://{BUCKET_NAME}/{FILE_KEY}', ignore_errors=true)
        WHERE upper(role) LIKE '%ПРОГРАММИСТ%' AND upper(name) LIKE '%1С%' 
          AND area IN ('Москва', 'Санкт-Петербург', 'Якутск')
        GROUP BY area 
        ORDER BY "Медианная ЗП (руб.)" DESC NULLS LAST;
    """).fetchdf()

    print("\n>>> СВОДНАЯ СТАТИСТИКА ПО РЕГИОНАМ:")
    print(df_reg.to_string(index=False))

    print("\n>>> ВОСТРЕБОВАННОСТЬ НАВЫКОВ ПО РЕГИОНАМ:")
    for region in regions:
        top_skills = con.execute(f"""
            WITH reg_skills AS (
                SELECT trim(skill) as skill
                FROM (
                    SELECT string_split(key_skills, ';') as skills_arr 
                    FROM read_csv_auto('s3://{BUCKET_NAME}/{FILE_KEY}', ignore_errors=true)
                    WHERE upper(role) LIKE '%ПРОГРАММИСТ%' AND upper(name) LIKE '%1С%' 
                      AND area = '{region}' AND key_skills IS NOT NULL
                ), UNNEST(skills_arr) as t(skill)
            )
            SELECT skill, count(*) as cnt
            FROM reg_skills
            WHERE skill != ''
            GROUP BY skill
            ORDER BY cnt DESC
            LIMIT 3;
        """).fetchall()

        skills_str = (
            ", ".join([f"{s} ({c})" for s, c in top_skills])
            if top_skills
            else "Нет данных по навыкам"
        )
        print(f"  - {region}: {skills_str}")
    print("=" * 50 + "\n")


if __name__ == "__main__":
    print("НАЧАЛО ПРОФИЛИРОВАНИЯ\n")

    mr_top = run_mapreduce()
    db_top = run_duckdb()

    print("\n=== ВЕРИФИКАЦИЯ РЕЗУЛЬТАТОВ ===")
    print("Топ-5 навыков MapReduce:", mr_top)
    print("Топ-5 навыков DuckDB:   ", db_top)

    if mr_top == db_top:
        print("-> УСПЕХ: Данные полностью совпадают!")
    else:
        print("-> ВНИМАНИЕ: Есть расхождения в данных!")

    analyze_salaries()
    analyze_regions()
