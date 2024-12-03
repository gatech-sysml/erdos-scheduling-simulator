import argparse
import subprocess
import time
import traceback
from pathlib import Path
from dataclasses import dataclass

SPARK_MIRROR_PATH = str(Path("../spark_mirror").resolve())
TPCH_SPARK_PATH = str(Path("../tpch-spark").resolve())


def bang(cmd, dry_run):
    cmd = [str(part) for part in cmd]
    print(" ".join(cmd))
    if dry_run:
        return
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return p


def must(cmd, dry_run):
    p = bang(cmd, dry_run)
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
    dry_run: bool

    def launch(self):
        must(
            [
                *("python3", "-m", "rpc.launch_tpch_queries"),
                *self.launcher_args,
                *("--spark-mirror-path", SPARK_MIRROR_PATH),
                *("--tpch-spark-path", TPCH_SPARK_PATH),
            ],
            self.dry_run,
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
            Launcher(self.launcher_args, self.args.dry_run).launch()
            time.sleep(10)


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

    experiments = [
        Experiment(
            name="testing",
            service_args=[
                "--enforce_deadlines",
                "--override_worker_cpu_count",
            ],
            launcher_args=["--num_queries", 1],
            args=args,
        )
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
