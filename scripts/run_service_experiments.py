import argparse
import subprocess
import time
import traceback
from pathlib import Path
from dataclasses import dataclass

SPARK_MIRROR_PATH = str(Path("../spark_mirror").resolve())
TPCH_SPARK_PATH = str(Path("../tpch-spark").resolve())


def bang(cmd, dry_run, stdout=subprocess.PIPE, stderr=subprocess.PIPE):
    cmd = [str(part) for part in cmd]
    print(" ".join(cmd))
    if dry_run:
        return
    p = subprocess.Popen(cmd, stdout=stdout, stderr=stderr)
    return p


def must(cmd, dry_run, stdout=subprocess.PIPE, stderr=subprocess.PIPE):
    p = bang(cmd, dry_run, stdout, stderr)
    if p.wait() != 0:
        stdout, stderr = p.communicate()
        raise Exception(f"Command failed. stdout: {stdout}. stderr: {stderr}.")
    return p


@dataclass
class Service:
    service_args: any
    output_dir: Path
    dry_run: bool

    _service = None
    _master = None
    _worker = None

    def __enter__(self):
        log_file = self.output_dir / "service.log"
        csv_file = self.output_dir / "service.csv"

        # launch service
        self._service = bang(
            [
                *("python3", "-m", "rpc.service"),
                *("--log", log_file),
                *("--csv_file_name", csv_file),
                *self.service_args,
            ],
            self.dry_run,
        )

        # sleep for some time
        if not self.dry_run:
            time.sleep(3)

        try:
            # launch spark master and worker
            self._master = must(
                [
                    f"{SPARK_MIRROR_PATH}/sbin/start-master.sh",
                    *("--host", "130.207.125.81"),
                    *(
                        "--properties-file",
                        f"{SPARK_MIRROR_PATH}/conf/spark-dg-config.conf",
                    ),
                ],
                self.dry_run,
            )
            self._worker = must(
                [
                    f"{SPARK_MIRROR_PATH}/sbin/start-worker.sh",
                    "spark://130.207.125.81:7077",
                    *(
                        "--properties-file",
                        f"{SPARK_MIRROR_PATH}/conf/spark-dg-config.conf",
                    ),
                ],
                self.dry_run,
            )
        except Exception as e:
            self.clean()
            raise e

        if not self.dry_run:
            time.sleep(5)

    def clean(self):
        if self._service:
            self._service.wait()
        if self._master:
            must([f"{SPARK_MIRROR_PATH}/sbin/stop-master.sh"], self.dry_run)
        if self._worker:
            must([f"{SPARK_MIRROR_PATH}/sbin/stop-worker.sh"], self.dry_run)

    def __exit__(self, type, value, traceback):
        self.clean()


@dataclass
class Launcher:
    launcher_args: any
    output_dir: Path
    dry_run: bool

    def launch(self):
        with (
            open(self.output_dir / "launcher.stdout", "w") as f_out,
            open(self.output_dir / "launcher.stderr", "w") as f_err,
        ):
            must(
                [
                    *("python3", "-u", "-m", "rpc.launch_tpch_queries"),
                    *self.launcher_args,
                    *("--spark-mirror-path", SPARK_MIRROR_PATH),
                    *("--tpch-spark-path", TPCH_SPARK_PATH),
                ],
                self.dry_run,
                stdout=f_out,
                stderr=f_err,
            )


@dataclass
class Experiment:
    name: str
    service_args: any
    launcher_args: any
    args: any

    def run(self):
        output_dir = self.args.output_dir / self.name
        if not output_dir.exists():
            output_dir.mkdir(parents=True)

        with Service(
            service_args=self.service_args,
            output_dir=output_dir,
            dry_run=self.args.dry_run,
        ) as s:
            Launcher(self.launcher_args, output_dir, self.args.dry_run).launch()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Prints commands that will be executed for each experiment",
    )
    parser.add_argument("--output-dir", type=Path, default=Path("exp-output"))
    args = parser.parse_args()

    if not args.output_dir.exists():
        args.output_dir.mkdir(parents=True)

    base_args = [
        "--enforce_deadlines",
        "--override_worker_cpu_count",
    ]
    variance_args = [
        *("--min_deadline_variance", 10),
        *("--max_deadline_variance", 25),
    ]
    edf_args = [
        *("--scheduler", "EDF"),
    ]
    dsched_args = [
        *("--scheduler", "TetriSched"),
        "--release_taskgraphs",
        *("--opt_passes", "CRITICAL_PATH_PASS"),
        *("--opt_passes", "CAPACITY_CONSTRAINT_PURGE_PASS"),
        *("--opt_passes", "DYNAMIC_DISCRETIZATION_PASS"),
        "--retract_schedules",
        *("--scheduler_max_occupancy_threshold", 0.999),
        "--finer_discretization_at_prev_solution",
        "--scheduler_selective_rescheduling",
        *("--scheduler_reconsideration_period", 0.6),
        *("--scheduler_time_discretization", 1),
        *("--scheduler_max_time_discretization", 5),
        *("--finer_discretization_window", 5),
        *("--scheduler_plan_ahead_no_consideration_gap", 1),
    ]
    experiments = [
        Experiment(
            name="edf-q300-hard",
            service_args=[
                *base_args,
                *edf_args,
                *variance_args,
            ],
            launcher_args=[
                *("--num_queries", 300),
                *("--variable_arrival_rate", 0.052),
            ],
            args=args,
        ),
        Experiment(
            name="dsched-q300-hard",
            service_args=[
                *base_args,
                *dsched_args,
                *variance_args,
            ],
            launcher_args=[
                *("--num_queries", 300),
                *("--variable_arrival_rate", 0.052),
            ],
            args=args,
        ),
    ]

    for i, experiment in enumerate(experiments):
        try:
            print(f"=== {experiment.name} ({i+1}/{len(experiments)}) ===")
            experiment.run()
            print("=== done ===")
        except Exception as e:
            print(traceback.format_exc())
            print(f"Failed to run experiment '{experiment}'. Exception: '{e}'")


if __name__ == "__main__":
    main()
