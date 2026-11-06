import argparse
import csv
import itertools
import json
import logging
import os
import sys
import time
from collections import defaultdict

from kazoo.client import KazooClient
from kazoo.exceptions import BadVersionError, NodeExistsError


def setup_logger(coordinator_id, log_file):
    logger = logging.getLogger(coordinator_id)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "[%(asctime)s.%(msecs)03d] [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    if log_file:
        os.makedirs(os.path.dirname(os.path.abspath(log_file)), exist_ok=True)
        fh = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True, help="Coordinator ID (coord1, coord2)")
    parser.add_argument("--fencing", type=int, default=0, help="0: no fencing, 1: epoch fencing")
    parser.add_argument("--hosts", default="localhost:2181,localhost:2182,localhost:2183")
    parser.add_argument("--csv", required=True, help="Path to it_vacancies_2024.csv")
    parser.add_argument("--log", default=None, help="Path to log file")
    args = parser.parse_args()

    logger = setup_logger(args.id, args.log)
    logger.info(f"Запуск координатора {args.id} (FENCING={args.fencing})")

    zk = KazooClient(hosts=args.hosts, timeout=10.0)
    zk.start()

    zk.ensure_path("/coordinator")
    leader_path = "/coordinator/leader"
    is_leader = False

    while not is_leader:
        try:
            zk.create(leader_path, args.id.encode("utf-8"), ephemeral=True)
            is_leader = True
            logger.info(f"Узел {args.id} захватил лидерство (создан эфемерный узел {leader_path})")
        except NodeExistsError:
            logger.info(f"Узел {args.id} в режиме ожидания (standby). Ожидание освобождения лидерства...")
            event = zk.handler.event_object()

            def watch_leader(watched_event):
                event.set()

            if zk.exists(leader_path, watch=watch_leader):
                event.wait()
            logger.info(f"Узел {args.id} зафиксировал смену лидера. Попытка стать лидером...")

    start_row = 0
    mapped_data = defaultdict(int)
    single_skills = defaultdict(int)

    if zk.exists("/coordinator/checkpoint"):
        cp_meta_data, _ = zk.get("/coordinator/checkpoint")
        if cp_meta_data:
            try:
                cp_meta = json.loads(cp_meta_data.decode("utf-8"))
                start_row = cp_meta.get("row_offset", 0)
                cp_file = cp_meta.get("checkpoint_file")
                if cp_file and os.path.exists(cp_file):
                    with open(cp_file, "r", encoding="utf-8") as f:
                        saved_state = json.load(f)
                        mapped_data.update(saved_state.get("mapped_data", {}))
                        single_skills.update(saved_state.get("single_skills", {}))
                logger.info(f"Успешно продолжен расчёт с чекпоинта: строка {start_row}")
            except Exception as e:
                logger.warning(f"Не удалось распарсить чекпоинт: {e}")

    expected_version = -1
    if args.fencing == 1:
        if zk.exists("/coordinator/result"):
            _, stat = zk.get("/coordinator/result")
            expected_version = stat.version
            logger.info(f"Зафиксирована базовая версия результата для Fencing: version={expected_version}")
        else:
            expected_version = 0

    logger.info(f"Начало обработки CSV с позиции {start_row}...")
    current_row = 0
    checkpoint_interval = 50000
    os.makedirs("checkpoints", exist_ok=True)

    with open(args.csv, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        for row in reader:
            current_row += 1
            if current_row <= start_row:
                continue

            role = row.get("role", "")
            name = row.get("name", "")
            if "ПРОГРАММИСТ" in role.upper() and "1С" in name.upper():
                skills_str = row.get("key_skills", "")
                if skills_str:
                    skills = sorted([s.strip() for s in skills_str.split(";") if s.strip()])
                    for s in skills:
                        single_skills[s] += 1
                    for pair in itertools.combinations(skills, 2):
                        mapped_data[f"{pair[0]} + {pair[1]}"] += 1

            if current_row % checkpoint_interval == 0:
                cp_file = os.path.abspath(f"checkpoints/cp_{args.id}_{current_row}.json")
                with open(cp_file, "w", encoding="utf-8") as f:
                    json.dump({"mapped_data": mapped_data, "single_skills": single_skills}, f)

                cp_meta = {
                    "row_offset": current_row,
                    "coordinator": args.id,
                    "checkpoint_file": cp_file,
                    "timestamp": time.time(),
                }
                try:
                    if not zk.exists("/coordinator/checkpoint"):
                        zk.create("/coordinator/checkpoint", json.dumps(cp_meta).encode("utf-8"))
                    else:
                        zk.set("/coordinator/checkpoint", json.dumps(cp_meta).encode("utf-8"))
                    logger.info(f"Сохранён чекпоинт в ZooKeeper: обработано строк {current_row}")
                except Exception as e:
                    logger.warning(f"Ошибка сохранения чекпоинта: {e}")

    logger.info(f"Расчёт завершён. Всего обработано строк: {current_row}")

    top_single = sorted(single_skills.items(), key=lambda x: x[1], reverse=True)[:5]
    top_pairs = sorted(mapped_data.items(), key=lambda x: x[1], reverse=True)[:5]

    logger.info(f"Топ-5 одиночных навыков: {top_single}")
    logger.info(f"Топ-5 пар навыков: {top_pairs}")

    result_payload = {
        "author": args.id,
        "fencing": args.fencing,
        "total_rows": current_row,
        "top_single": top_single,
        "top_pairs": top_pairs,
        "saved_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    result_bytes = json.dumps(result_payload, ensure_ascii=False, indent=2).encode("utf-8")

    if args.fencing == 0:
        try:
            if not zk.connected:
                logger.warning(f"Сессия {args.id} истекла во время паузы. Переподключение для слепой записи без защиты...")
                zk_client = KazooClient(hosts=args.hosts, timeout=10.0)
                zk_client.start()
            else:
                zk_client = zk

            if not zk_client.exists("/coordinator/result"):
                zk_client.create("/coordinator/result", result_bytes)
                stat = zk_client.exists("/coordinator/result")
            else:
                stat = zk_client.set("/coordinator/result", result_bytes)
            logger.info(f"{args.id} записал результат (версия {stat.version})")
            if zk_client != zk:
                zk_client.stop()
        except Exception as e:
            logger.error(f"Ошибка записи без защиты: {e}")
    else:
        try:
            if not zk.connected:
                logger.warning(f"Сессия {args.id} истекла. Переподключение для попытки записи с защитой (version={expected_version})...")
                zk_client = KazooClient(hosts=args.hosts, timeout=10.0)
                zk_client.start()
            else:
                zk_client = zk

            stat = zk_client.set("/coordinator/result", result_bytes, version=expected_version)
            logger.info(f"{args.id} записал результат (версия {stat.version}, указатель от {args.id})")
            if zk_client != zk:
                zk_client.stop()
        except BadVersionError:
            logger.error(f"{args.id} ошибка публикации: BadVersionError: версия {expected_version} устарела! Запись отклонена (Fencing сработал).")
        except Exception as e:
            logger.error(f"{args.id} непредвиденная ошибка публикации: {e}")

    try:
        zk.stop()
        zk.close()
    except Exception:
        pass


if __name__ == "__main__":
    main()
