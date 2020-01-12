# python 3.7
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
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


def get_suboutput(output, name):
    if name not in output or not isinstance(output[name], dict):
        output[name] = {}
    return output[name]


def make_suboutput(output, name):
    output[name] = {}
    return output[name]


def output_file(output, data, name):
    file_name = name + '.csv'
    data.to_csv(path_in_data(file_name))
    output[name] = file_name

def set_none_to(value):
    return lambda x: x if x is not None else value

def do_analysis(config, messages, output):
    print(messages.columns)
    messages = messages.sort_values(by='date')
    output['first_message'] = messages.iloc[0]['date_utc'].strftime("%Y-%m-%d %H:%M:%S")

    messages['from_alex'] = messages['is_from_me'] == 1
    messages['from_andy'] = messages['is_from_me'] == 0

    count_messages(messages, output)
    count_messages_by_week(messages, output)
    analyze_words(config['words'], messages, output)

    plt.show()


def analyze_words(to_analyze, messages, output):
    output = make_suboutput(output, 'word_analysis')
    words = pd.DataFrame(messages[['date_utc', 'text', 'from_alex', 'from_andy']])
    words['list'] = (words['text']
        .str.lower()
        .str.split(' ')
        .apply(set_none_to([]))
    )
    words["text"] = words["text"].apply(set_none_to(""))

    value_output = make_suboutput(output, "words")
    for word, analysis_type in to_analyze.items():
        analyze_word(words, word, analysis_type, value_output)

    words_by_week = sum_by_month(words.drop(['text', 'from_alex', 'from_andy'], axis=1))

    output_file(output, words_by_week, "words_by_week")

    words_by_week.plot(kind="line")

def analyze_word(words, word, analysis_type, output):
    output = get_suboutput(output, word)
    count_word(words, word, analysis_type)
    output['total'] = int(words[word].sum())
    output['from_alex'] = int(words[words['from_alex']][word].sum())
    output['from_andy'] = int(words[words['from_andy']][word].sum())
    output['max_per_message'] = int(words[word].max())
    output['avg_per_message'] = words[word].mean()

def count_word(words, word, analysis_type):
    finder = get_finder(word, analysis_type)
    field = get_field(analysis_type)
    words[word] = words[field].apply(finder)

def get_finder(word, analysis_type):
    word = word.lower()
    if analysis_type == "substring":
        return lambda tokens: find_substring(tokens, word)
    elif analysis_type == "word":
        return lambda tokens: find_word(tokens, word)
    elif analysis_type == "phrase":
        return lambda text: 1 if word in text else 0
    else:
        raise KeyError("invalid analysis type")

def get_field(analysis_type):
    if analysis_type == "phrase":
        return "text"
    else:
        return "list"

def find_substring(tokens, word):
    return sum(map(lambda token: 1 if word in token else 0, tokens))

def find_word(tokens, word):
    return sum(map(lambda token: 1 if word == token else 0, tokens))

def count_messages_by_week(messages, output):
    weekly_msgs = sum_by_week(messages)[['from_alex', 'from_andy']]
    weekly_msgs['diff'] = weekly_msgs['from_alex'] - weekly_msgs['from_andy']

    output_file(output, weekly_msgs, "weekly_message_count")

def count_messages(messages, output):
    output["total_messages"] = len(messages)
    from_alex = messages[messages['is_from_me'] == 1]
    from_andy = messages[messages['is_from_me'] == 0]

    output['messages_from_alex'] = len(from_alex)
    output['messages_from_andy'] = len(from_andy)

def sum_by_week(messages):
    return messages.groupby([pd.Grouper(key='date_utc', freq='W-MON', label='left')]).sum()

def sum_by_month(messages):
    return messages.groupby([pd.Grouper(key='date_utc', freq='M', label='left')]).sum()


def main(messages_pickle, output_json, config):
    messages_file = path_in_data(messages_pickle)
    output_file = path_in_data(output_json)
    messages = pd.read_pickle(messages_file)
    with OutputContext(output_file) as output:
        do_analysis(config, messages, output)


if __name__ == "__main__":
    with open(CONFIG_FILE) as f:
        config = json.load(f)
    main(
        messages_pickle=config['messages_pickle'],
        output_json=config['output'],
        config=config['analysis']
    )


