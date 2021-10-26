import os
import subprocess

HOME = "/home/pawngrubber"
LOG = f"{HOME}/log.txt"

cron_command = (
    f'(crontab -l 2>/dev/null; echo "@reboot cd {HOME}/grubberbot '
    f'&& sudo /usr/bin/python3.8 {HOME}/grubberbot/reboot.py")| crontab -'
)


def main():
    pass


if __name__ == "__main__":
    main()
