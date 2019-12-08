# python 3.7
import sqlite3
import pandas as pd
import json
import os.path

DATA_PATH = "../data/{file_name}"
CONFIG_FILE = DATA_PATH.format(file_name="config.json")


class OutputContext:
    def __init__(self, path):
        self.path = path
        self.output = {}

    def __enter__(self):
        if os.path.exists(self.path):
            with open(self.path) as f:
                self.output = json.load(f)
        else:
            self.output = {}
        return self.output

    def __exit__(self, *args):
        with open(self.path, "w+") as f:
            output = json.dumps(self.output, sort_keys=True, indent=2)
            f.write(output)


def path_in_data(file_name):
    return DATA_PATH.format(file_name=file_name)


def do_analysis(messages, output):
    output["total_messages"] = len(messages)
    from_alex = messages[messages['is_from_me'] == 1]
    from_andy = messages[messages['is_from_me'] == 0]

    output['messages_from_alex'] = len(from_alex)
    output['messages_from_andy'] = len(from_andy)

    messages = messages.sort_values(by='date')

    output['first_message'] = messages.iloc[0]['date_utc'].strftime("%Y-%m-%d %H:%M:%S")


def main(messages_pickle, output_json):
    messages_file = path_in_data(messages_pickle)
    output_file = path_in_data(output_json)
    messages = pd.read_pickle(messages_file)
    with OutputContext(output_file) as output:
        do_analysis(messages, output)


if __name__ == "__main__":
    with open(CONFIG_FILE) as f:
        config = json.load(f)
    main(
        messages_pickle=config['messages_pickle'],
        output_json=config['output']
    )



