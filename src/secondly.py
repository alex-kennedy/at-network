import configparser
import json
import time

import requests

REALTIME_API = 'https://api.at.govt.nz/v2/public/realtime'

def get_headers(path='.credentials/key.conf'):
    """Get the authentication key header for all requests"""

    config = configparser.ConfigParser()
    config.read(path)

    headers = {
        'Ocp-Apim-Subscription-Key': config['default']['primary']
    }

    return headers


def request_and_save(now, headers):
    """Make one request and save the result"""
    r = requests.get(REALTIME_API, headers=headers)

    if r.status_code != 200:
        return False

    with open('data/realtime_combined_feed_{}.json'.format(now), 'w') as out:
        out.write(r.text)
    
    return True


def keep_requesting():
    """Indefinitely make requests every 20 seconds, saving the results"""
    
    headers = get_headers()

    while True:
        now = time.time()
        now_str = str(round(now))

        status = request_and_save(now_str, headers)

        print('Request at {} gave {}'.format(now_str, status))
        log.write('{},{}\n'.format(now_str, status))

        time_diff = time.time() - now
        if time_diff < 20:
            time.sleep(20 - time_diff)


def resilient_requesting(start, try_number=1):
    try:
        keep_requesting()
    except Exception as e:
        if time.time() - start > 600:
            resilient_requesting(time.time(), try_number=1)
        elif try_number <= 4:
            time.sleep(2 ** try_number)
            resilient_requesting(time.time(), try_number=try_number + 1)
        else:
            print('It got mad...')
            raise e


if __name__ == '__main__':
    resilient_requesting(time.time(), try_number=1)
