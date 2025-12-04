import argparse
import subprocess

from datetime import datetime

def main():

    parser = argparse.ArgumentParser()

    parser.add_argument('--mode', type=str, required=True, choices=['local', 'remote', 'test'])

    parser.add_argument('--date', type=str, required=False, default=datetime.now().strftime('%y%m%d'))

    args = parser.parse_args()

    mode = args.mode ; date = args.date

    subprocess.run(['python', '-m', 'scripts.upsert_surveys', '--mode', mode, '--date', date], check=True)
    subprocess.run(['python', '-m', 'scripts.recruited', '--mode', mode], check=True)
    subprocess.run(['python', '-m', 'scripts.historical', '--mode', mode], check=True)

if __name__ == '__main__':
    main()