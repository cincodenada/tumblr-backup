import argparse
import os, sys
import sqlite3
from time import sleep
from datetime import datetime, timedelta
import json
import csv

import requests

# PATH to file with api key
CONFIG = '../config.txt'

# Be nice to tumblr servers
RATE_LIMIT = 3  # seconds


class json_getter():
    def __init__(self, blog, api_key, rate_limit = 3):
        self.url = (f'https://api.tumblr.com/v2/blog/{blog}.tumblr.com/posts/'
           f'?api_key={api_key}')
        self.last_request = datetime.now()
        self.rate_limit = timedelta(seconds=rate_limit)

    def get(self, offset):
        """GET JSON from Tumblr

        :param url: Tumblr API url with appropriate offset param
        :returns: JSON response (dict)

        """
        cachefile = os.path.join('cache',str(offset) + '.json')
        try:
            return json.load(open(cachefile, 'r'))
        except FileNotFoundError:
            json_dict = self._get(offset)
            json.dump(json_dict, open(cachefile, 'w'))
            return json_dict

    def _get(self, offset):
        print("Loading from remote...")
        self.rate_wait()

        url = f'{self.url}&offset={offset}'
        resp = requests.get(url, timeout=10)
        if resp.status_code == 404:
            raise SystemExit('\n[!] Blog is 404\n')

        try:
            return resp.json()['response']
        except (KeyError, ValueError):
            raise SystemExit('\n[!] Error getting JSON\n')

    def rate_wait(self):
        wait_until = self.last_request + self.rate_limit
        sleep_time = wait_until - datetime.now()
        if(sleep_time.total_seconds() > 0):
            sleep(sleep_time.total_seconds())

        self.last_request = datetime.now()


def main():
    """Main logic method"""
    args = parse_args()
    blog = args.blog
    offset = args.offset

    api_key = parse_api_key()
    mkdir('output')
    mkdir('cache')

    db = Database(os.path.join('output',blog))
    getter = json_getter(blog, api_key, RATE_LIMIT)

    while True:
        try:
            js = getter.get(offset)
            if not remaining_json(js, db):
                return print('\nFinished\n')
            else:
                offset += 20
                print(f'\noffset {offset}\n')
        except Exception as e:
            print("Problem json:")
            print(js)
            raise e

        sys.stdout.flush()


def parse_args():
    """Parse CLI args

    :returns: args (object)

    """
    parser = argparse.ArgumentParser(
                        description='Backup tumblr blog')
    parser.add_argument('blog',
                        help='Tumblr blog')
    parser.add_argument('-o', '--offset',
                        help='Post offset',
                        default=0,
                        type=int)
    args = parser.parse_args()
    return args


def parse_api_key():
    """Parse API key from CONFIG file

    :returns: API key (string)

    """
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    try:
        with open(CONFIG) as file:
            api_key = file.readline().strip()
            return api_key
    except FileNotFoundError:
        raise SystemExit(f'\n[!] CONFIG file "{CONFIG}" missing\n')


def remaining_json(js, db):
    """Parse JSON

    :param js: JSON response (dict)
    :param db: Databse object
    :returns: True if there are more posts to fetch, else close out DB

    """
    post_count = len(js['posts'])

    for post in range(post_count):
        post_data = js['posts'][post]

        post_type = post_data['type']
        post_id = post_data['id']
        date = post_data['date']
        notes = post_data['note_count']
        tags = ','.join(post_data['tags'])
        is_reblog = ('trail' in post_data)

        try:
            if 'photo' in post_type:
                data_2 = post_data['caption']
                photo_count = len(post_data['photos'])

                if photo_count > 1:  # photoset
                    photos = []
                    for photo in range(photo_count):
                        img = post_data['photos'][photo]['original_size']['url']
                        photos.append(img)
                    data_1 = ','.join(photos)  # convert to string for db
                    post_type = 'photoset'  # manual correction
                else:  # photo
                    data_1 = post_data['photos'][0]['original_size']['url']

            elif 'video' in post_type:
                data_2 = post_data['caption']
                video_type = post_data['video_type']

                if 'instagram' in video_type:
                    data_1 = post_data['permalink_url']
                elif 'tumblr' in video_type:
                    data_1 = post_data['video_url']
                elif 'youtube' in video_type:
                    video_id = post_data['video']['youtube']['video_id']
                    data_1 = f'https://www.youtube.com/watch?v={video_id}'
                else:
                    data_1 = None

            elif 'answer' in post_type:
                data_1 = post_data['question']
                data_2 = post_data['answer']

            elif 'text' in post_type:
                data_1 = post_data['title']
                data_2 = post_data['body']

            elif 'quote' in post_type:
                data_1 = post_data['text']
                data_2 = post_data['source']

            elif 'link' in post_type:
                data_1 = post_data['title']
                data_2 = post_data['url']

            elif 'audio' in post_type:
                data_1 = post_data['source_url']
                data_2 = post_data['caption']

            elif 'chat' in post_type:
                data_1 = post_data['title']
                data_2 = post_data['body']

        except KeyError:
            print("Couldn't find details for {} post!".format(post_type))
            print(post_data)
            data_1 = None
            data_2 = None

        db.write(post_type, post_id, date, notes, tags, is_reblog, data_1, data_2)

    if post_count or post_count >= 20:
        return True
    else:
        db.commit()


def mkdir(dir):
    """Make local output folder"""
    try:
        os.mkdir(dir)
        print(f'\n[+] Creating {dir} folder')
    except FileExistsError:
        pass
    except PermissionError:
        raise SystemExit(f'\n[!] Can\'t create directory "{dir}"\n')


class Database:

    def __init__(self, path):
        """Initialize local database while checking for an existing one

        :param blog: Tumblr blog name

        """
        db = f'{path}.db'
        self.check_backup(db)

        try:
            self.conn = sqlite3.connect(db)
            print(f'[+] Creating new DB "{db}"\n')
        except sqlite3.OperationalError:
            raise SystemExit(f'\n[!] Can\'t create DB {db}\n')

        self.cur = self.conn.cursor()
        self.create_tables()

        self.csv = csv.writer(open(f'{path}.csv', 'w'))
        self.csv.writerow(['post_type', 'post_id', 'date', 'notes', 'tags', 'is_reblog', 'data_1', 'data_2'])

    def check_backup(self, db):
        """Create backup of DB if DB already exists"""
        if os.path.isfile(db):
            try:
                os.rename(db, f'{db}.bak')
                print(f'\n[+] Backing up old DB "{db}.bak"')
            except OSError:
                print('\n[!] Can\'t create backup\n')

    def create_tables(self):
        """Create DB tables"""
        self.cur.execute("""CREATE TABLE photo(post_id INT,
                                               date TEXT,
                                               notes INT,
                                               tags TEXT,
                                               is_reblog INT,
                                               img TEXT,
                                               caption TEXT)""")

        self.cur.execute("""CREATE TABLE photoset(post_id INT,
                                                  date TEXT,
                                                  notes INT,
                                                  tags TEXT,
                                                  is_reblog INT,
                                                  photoset TEXT,
                                                  caption TEXT)""")

        self.cur.execute("""CREATE TABLE text(post_id INT,
                                              date TEXT,
                                              notes INT,
                                              tags TEXT,
                                              is_reblog INT,
                                              title TEXT,
                                              body TEXT)""")

        self.cur.execute("""CREATE TABLE answer(post_id INT,
                                                date TEXT,
                                                notes INT,
                                                tags TEXT,
                                                is_reblog INT,
                                                question TEXT,
                                                answer TEXT)""")

        self.cur.execute("""CREATE TABLE video(post_id INT,
                                               date TEXT,
                                               notes INT,
                                               tags TEXT,
                                               is_reblog INT,
                                               video_url TEXT,
                                               caption TEXT)""")

        self.cur.execute("""CREATE TABLE quote(post_id INT,
                                               date TEXT,
                                               notes INT,
                                               tags TEXT,
                                               is_reblog INT,
                                               quote TEXT,
                                               source TEXT)""")

        self.cur.execute("""CREATE TABLE link(post_id INT,
                                              date TEXT,
                                              notes INT,
                                              tags TEXT,
                                              is_reblog INT,
                                              title TEXT,
                                              url TEXT)""")

        self.cur.execute("""CREATE TABLE audio(post_id INT,
                                               date TEXT,
                                               notes INT,
                                               tags TEXT,
                                               is_reblog INT,
                                               url TEXT,
                                               caption TEXT)""")

        self.cur.execute("""CREATE TABLE chat(post_id INT,
                                              date TEXT,
                                              notes INT,
                                              tags TEXT,
                                              is_reblog INT,
                                              title TEXT,
                                              body TEXT)""")

        self.cur.execute("""CREATE VIEW all_posts(type, post_id, date, notes, tags, is_reblog, data1, data2) AS
                                SELECT 'photo', * FROM photo UNION ALL
                                SELECT 'photoset', * FROM photoset UNION ALL
                                SELECT 'text', * FROM text UNION ALL
                                SELECT 'answer', * FROM answer UNION ALL
                                SELECT 'video', * FROM video UNION ALL
                                SELECT 'quote', * FROM quote UNION ALL
                                SELECT 'link', * FROM link UNION ALL
                                SELECT 'audio', * FROM audio UNION ALL
                                SELECT 'chat', * FROM chat;""")

    def write(self, post_type, post_id, date, notes, tags, is_reblog, data_1, data_2):
        """Each post type will have 2 additional values for the db.

        data_1 & data_2
        ---------------
        photo: img, caption
        photoset: photoset, caption
        video: video_url, caption
        answer: question, answer
        text: title, body
        quote: quote, source
        link: title, url
        audio: url, caption
        chat: title, body

        """
        print(f'{post_type} - #{post_id}')
        self.cur.execute(f'INSERT INTO {post_type} VALUES(?, ?, ?, ?, ?, ?, ?)',
                         (post_id, date, notes, tags, is_reblog, data_1, data_2))

        self.csv.writerow([post_type, post_id, date, notes, tags, is_reblog, data_1, data_2])

    def commit(self):
        """Commmit everything and close out the DB

        We *could* commit after every write, in self.write() above,
        but that's really slow.

        """
        self.conn.commit()
        self.conn.close()


if __name__ == '__main__':

    main()
