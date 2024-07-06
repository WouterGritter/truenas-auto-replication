import subprocess
import json
import time
import os

import requests


def load_dotenv():
    # Get the directory where the current script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    dotenv_path = os.path.join(script_dir, '.env')

    if os.path.exists(dotenv_path):
        with open(dotenv_path, 'r') as file:
            for line in file:
                # Strip leading/trailing whitespace and ignore empty lines or comments
                line = line.strip()
                if line and not line.startswith('#'):
                    # Split at the first '=' and strip any extra whitespace
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip()

                    # Remove surrounding quotes from value if they exist
                    if value.startswith('"') and value.endswith('"'):
                        value = value[1:-1]
                    elif value.startswith("'") and value.endswith("'"):
                        value = value[1:-1]

                    # Set the environment variable
                    os.environ[key] = value
    else:
        raise FileNotFoundError(f"No .env file found in the directory: {script_dir}")


load_dotenv()

DISCORD_WEBHOOK_URL = os.getenv('DISCORD_WEBHOOK_URL')
ZPOOL_NAME = os.getenv('ZPOOL_NAME')
MIN_EXECUTION_TIME = float(os.getenv('MIN_EXECUTION_TIME', 90.0))

print(f'{DISCORD_WEBHOOK_URL=}')
print(f'{ZPOOL_NAME=}')


def query_replications():
    command = ['midclt', 'call', 'replication.query']
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        print(f'{result.stdout=}')
        print(f'{result.stderr=}')
        raise Exception(f'Process finished with exit code {result.returncode}.')

    return json.loads(result.stdout)


def get_running_replications():
    return [r for r in query_replications() if r['state']['state'] == 'RUNNING']


def wait_for_running_replications():
    while len(get_running_replications()) > 0:
        print('Waiting for replication(s) to finish...')
        time.sleep(5)


def start_replication(replication_id):
    command = ['midclt', 'call', 'replication.run', str(replication_id)]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        print(f'{result.stdout=}')
        print(f'{result.stderr=}')
        raise Exception(f'Process finished with exit code {result.returncode}.')


def get_zpool_usage():
    # Run the 'zpool list' command and capture the output
    result = subprocess.run(['zpool', 'list'], stdout=subprocess.PIPE, text=True)

    # Parse the output
    lines = result.stdout.strip().split('\n')
    headers = lines[0].split()
    pools = lines[1:]

    # Create the dictionary
    pools_dict = {}
    for pool in pools:
        values = pool.split()
        pool_name = values[0]
        pool_data = {header.lower(): value for header, value in zip(headers, values)}
        pools_dict[pool_name] = pool_data

    return pools_dict


def shutdown_system():
    os.system('poweroff')


def format_time(seconds):
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    time_components = []
    if hours > 0:
        time_components.append(f'{hours:.0f} hours')
    if minutes > 0:
        time_components.append(f'{minutes:.0f} minutes')
    if seconds > 0 or not time_components:
        time_components.append(f'{seconds:.1f} seconds')

    if len(time_components) > 1:
        return ', '.join(time_components[:-1]) + ' and ' + time_components[-1]
    else:
        return time_components[0]


def send_discord_message(message):
    if DISCORD_WEBHOOK_URL is None:
        return

    requests.post(
        DISCORD_WEBHOOK_URL,
        json={
            'content': message
        }
    )


def main():
    start = time.time()

    replications = query_replications()
    print(f'Found {len(replications)} replications.')
    for replication in replications:
        print(f'Running replication #{replication["id"]} ({replication["name"]})...')
        start_replication(replication['id'])
        wait_for_running_replications()
        print(f'Replication #{replication["id"]} ({replication["name"]}) has finished!')

    print('All replications have finished.')

    elapsed = time.time() - start

    # Ensure the system is on for at least MIN_EXECUTION_TIME seconds, to allow the creation of the 'no_shutdown' file by the server administrator.
    time.sleep(max(0.0, MIN_EXECUTION_TIME - elapsed))

    perform_shutdown = not os.path.exists('no_shutdown')

    pool_usage = get_zpool_usage()[ZPOOL_NAME]
    discord_message = (f'Performed `{len(replications)}` replications in `{format_time(elapsed)}`.\n'
                       f'Pool usage: `{pool_usage["cap"]}` (`{pool_usage["free"]}` free of `{pool_usage["size"]}`)\n'
                       f'Health: `{pool_usage["health"]}`')
    if perform_shutdown:
        discord_message += '\n\nShutting down the system now.'
    send_discord_message(discord_message)

    if perform_shutdown:
        print('Shutting down the system...')
        shutdown_system()
    else:
        print('no_shutdown file is present. Not shutting down the system.')


if __name__ == '__main__':
    main()
