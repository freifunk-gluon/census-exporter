#!/usr/bin/env python3

import json
import requests
import structlog

def download(url, timeout=5):
    return requests.get(url, timeout=timeout)

log = structlog.get_logger()

def main():
    # Open and load the JSON file
    with open('communities.json', 'r') as f:
        data = json.load(f)

    # Iterate over the JSON object
    for community, urls in data.items():
        for i, url in enumerate(urls):
            urlSplit = url.rsplit('/', 1)

            # check assumption
            if urlSplit[1] not in ['nodes.json', 'meshviewer.json']:
                log.msg(f"{community}: The URL doesn't contain a supported filename, skipping", url=url)
                continue

            # get url without file
            urlPath = urlSplit[0]

            # Send a GET request to test whether nodes.json is available
            urlTest = urlPath + '/nodes.json'
            try:
                response = download(urlTest)
            except requests.exceptions.RequestException as e:
                response.status_code = -1

            # Check the status code of the response
            if response.status_code == 200:
                print(f'{community}: OK: nodes.json available')
                data[community][i] = urlTest
            else:
                # nodes.json is not available, try meshviewer.json
                urlTest = urlPath + '/meshviewer.json'
                try:
                    response = download(urlTest)
                    if response.status_code == 200:
                        data[community][i] = urlTest
                        print(f'{community}: Fallback: meshviewer.json available')
                    else:
                        # neither is available, print error and keep the original URL
                        log.msg(f"'{community}: Unexpected HTTP status code", status_code=response.status_code, url=url)
                except requests.exceptions.RequestException as e:
                    # neither is reachable, print error and keep the original URL
                    log.msg(f"{community}: Exception caught while fetching {urlTest}", ex=e)

    print('\nDone. Writing to communities.json')
    with open('communities.json', 'w') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write('\n')

if __name__ == "__main__":
    main()