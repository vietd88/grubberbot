import os
import subprocess

HOME = "/home/pawngrubber"
LOG = f"{HOME}/log.txt"
COMMANDS = [
    "sudo git fetch --all",
    "sudo git reset --hard origin/production",
    "sudo python3.8 -m pip install -r requirements-dev.txt",
    "sudo docker image prune --force --all",
    (
        "sudo docker-compose -f docker-compose-production.yml up "
        "--build --detach --force-recreate --remove-orphans"
    ),
]
COMMANDS = [f"cd {HOME}/grubberbot; {c}" for c in COMMANDS]


def main():
    with open(LOG, "w") as f:
        print("", file=f)
    for command in COMMANDS:
        output = subprocess.run(
            command,
            shell=True,
            cwd=".",
            capture_output=True,
        )
        with open(LOG, "a") as f:
            print(output, file=f)


if __name__ == "__main__":
    main()
