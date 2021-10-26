import datetime
import time

import funcs_google as fgg


def main():
    time.sleep(1)
    print("Test development things here")
    fgg.backup_db()
    fgg.download_db()


if __name__ == "__main__":
    main()
