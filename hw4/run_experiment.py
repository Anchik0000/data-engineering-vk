import argparse
import json
import os
import subprocess
import sys
import time

import psutil
from kazoo.client import KazooClient


def run(fencing):
    print("=" * 70)
    print(f"ЗАПУСК ЭКСПЕРИМЕНТА С ПАРАМЕТРОМ FENCING={fencing}")
    print("=" * 70)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    hosts = "localhost:2181,localhost:2182,localhost:2183"
    csv_path = os.path.join(script_dir, "it_vacancies_2024.csv")
    if not os.path.exists(csv_path):
        csv_path = os.path.abspath(os.path.join(script_dir, "..", "it_vacancies_2024.csv"))
    coord_script = os.path.join(script_dir, "coordinator.py")
    log_file = os.path.join(script_dir, "logs", f"coordinator_fencing_{fencing}.log")

    if os.path.exists(log_file):
        os.remove(log_file)

    zk = KazooClient(hosts=hosts, timeout=10.0)
    zk.start()

    if zk.exists("/coordinator"):
        zk.delete("/coordinator", recursive=True)

    zk.ensure_path("/coordinator")
    zk.ensure_path("/coordinator/checkpoint")
    zk.create("/coordinator/result", b'{"status": "init"}')
    _, init_stat = zk.get("/coordinator/result")
    print(f"[ORCHESTRATOR] Инициализирован узел /coordinator/result (начальная версия {init_stat.version})")

    cmd1 = [
        sys.executable,
        coord_script,
        "--id",
        "coord1",
        "--fencing",
        str(fencing),
        "--hosts",
        hosts,
        "--csv",
        csv_path,
        "--log",
        log_file,
    ]
    p1 = subprocess.Popen(cmd1, cwd=script_dir)
    print(f"[ORCHESTRATOR] Запущен Лидер (coord1, PID={p1.pid})")

    time.sleep(1.0)

    cmd2 = [
        sys.executable,
        coord_script,
        "--id",
        "coord2",
        "--fencing",
        str(fencing),
        "--hosts",
        hosts,
        "--csv",
        csv_path,
        "--log",
        log_file,
    ]
    p2 = subprocess.Popen(cmd2, cwd=script_dir)
    print(f"[ORCHESTRATOR] Запущен Запасной координатор (coord2, PID={p2.pid})")

    print("[ORCHESTRATOR] Ожидание достижения чекпоинта (50 000 строк) лидером coord1...")
    while True:
        time.sleep(0.5)
        if zk.exists("/coordinator/checkpoint"):
            cp_data, _ = zk.get("/coordinator/checkpoint")
            if cp_data:
                try:
                    cp = json.loads(cp_data.decode("utf-8"))
                    if cp.get("row_offset", 0) >= 50000:
                        print(f"[ORCHESTRATOR] Чекпоинт достигнут ({cp.get('row_offset')} строк). Приостанавливаем лидера!")
                        break
                except Exception:
                    pass

    ps1 = psutil.Process(p1.pid)
    ps1.suspend()
    t_pause = time.time()
    print(f"[ORCHESTRATOR] >>> ЛИДЕР coord1 ПРИОСТАНОВЛЕН в {time.strftime('%H:%M:%S', time.localtime(t_pause))} на 40 секунд <<<")

    leader_taken = False
    failover_time = None
    while not leader_taken:
        time.sleep(0.1)
        if zk.exists("/coordinator/leader"):
            data, _ = zk.get("/coordinator/leader")
            if data and data.decode("utf-8") == "coord2":
                t_takeover = time.time()
                leader_taken = True
                failover_time = t_takeover - t_pause
                print(f"[ORCHESTRATOR] >>> ВНИМАНИЕ: Запасной coord2 стал ЛИДЕРОМ! <<<")
                print(f"[ORCHESTRATOR] >>> ИЗМЕРЕННОЕ ВРЕМЯ СМЕНЫ ЛИДЕРА (FAILOVER TIME): {failover_time:.2f} сек. <<<")

    elapsed = time.time() - t_pause
    if elapsed < 40.0:
        time.sleep(40.0 - elapsed)

    ps1.resume()
    t_resume = time.time()
    print(f"[ORCHESTRATOR] >>> ЛИДЕР coord1 ВОЗОБНОВЛЁН в {time.strftime('%H:%M:%S', time.localtime(t_resume))} (прошло {t_resume - t_pause:.1f} сек.) <<<")

    p1.wait()
    p2.wait()

    res_data, res_stat = zk.get("/coordinator/result")
    result_json = json.loads(res_data.decode("utf-8"))

    print("\n" + "=" * 70)
    print("ИТОГИ ЭКСПЕРИМЕНТА:")
    print(f"Измеренное время failover: {failover_time:.2f} сек.")
    print(f"Версия узла /coordinator/result (dataVersion): {res_stat.version}")
    print(f"Автор итогового результата: {result_json.get('author')}")
    print(f"Топ-5 одиночных навыков: {result_json.get('top_single')}")
    print(f"Топ-5 пар навыков: {result_json.get('top_pairs')}")
    print("=" * 70 + "\n")

    zk.stop()
    zk.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fencing", type=int, choices=[0, 1], required=True)
    args = parser.parse_args()
    run(args.fencing)
