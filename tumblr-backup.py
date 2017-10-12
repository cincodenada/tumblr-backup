import argparse
import os, sys
import sqlite3
from time import sleep

import requests

# PATH to file with api key
CONFIG = '../config.txt'

# Be nice to tumblr servers
RATE_LIMIT = 3  # seconds


def main():
    """Main logic method"""
    args = parse_args()
    blog = args.blog
    offset = args.offset

    api_key = parse_api_key()
    url = (f'https://api.tumblr.com/v2/blog/{blog}.tumblr.com/posts/'
           f'?api_key={api_key}')

    mkdir_output()
    db = Database(blog)

    while True:
        js = get_json(f'{url}&offset={offset}')
        if not remaining_json(js, db):
            return print('\nFinished\n')
        else:
            offset += 20
            print(f'\noffset {offset}\n')
            sleep(RATE_LIMIT)

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


def get_json(url):
    """GET JSON from Tumblr

    :param url: Tumblr API url with appropriate offset param
    :returns: JSON response (dict)

    """
    resp = requests.get(url, timeout=10)
    if resp.status_code == 404:
        raise SystemExit('\n[!] Blog is 404\n')

    try:
        return resp.json()['response']
    except (KeyError, ValueError):
        raise SystemExit('\n[!] Error getting JSON\n')


def remaining_json(js, db):
    """Parse JSON

    :param js: JSON response (dict)
    :param db: Databse object
    :returns: True if there are more posts to fetch, else close out DB

    """
    post_count = len(js['posts'])

    for post in range(post_count):
        post_type = js['posts'][post]['type']
        post_id = js['posts'][post]['id']
        date = js['posts'][post]['date']
        notes = js['posts'][post]['note_count']
        tags = ','.join(js['posts'][post]['tags'])

        try:
            if 'photo' in post_type:
                data_2 = js['posts'][post]['caption']
                photo_count = len(js['posts'][post]['photos'])

                if photo_count > 1:  # photoset
                    photos = []
                    for photo in range(photo_count):
                        img = js['posts'][post]['photos'][photo]['original_size']['url']
                        photos.append(img)
                    data_1 = ','.join(photos)  # convert to string for db
                    post_type = 'photoset'  # manual correction
                else:  # photo
                    data_1 = js['posts'][post]['photos'][0]['original_size']['url']

            elif 'video' in post_type:
                data_2 = js['posts'][post]['caption']
                video_type = js['posts'][post]['video_type']

                if 'instagram' in video_type:
                    data_1 = js['posts'][post]['permalink_url']
                elif 'tumblr' in video_type:
                    data_1 = js['posts'][post]['video_url']
                elif 'youtube' in video_type:
                    video_id = js['posts'][post]['video']['youtube']['video_id']
                    data_1 = f'https://www.youtube.com/watch?v={video_id}'
                else:
                    data_1 = None

            elif 'answer' in post_type:
                data_1 = js['posts'][post]['question']
                data_2 = js['posts'][post]['answer']

            elif 'text' in post_type:
                data_1 = js['posts'][post]['title']
                data_2 = js['posts'][post]['body']

            elif 'quote' in post_type:
                data_1 = js['posts'][post]['text']
                data_2 = js['posts'][post]['source']

            elif 'link' in post_type:
                data_1 = js['posts'][post]['title']
                data_2 = js['posts'][post]['url']

            elif 'audio' in post_type:
                data_1 = js['posts'][post]['source_url']
                data_2 = js['posts'][post]['caption']

            elif 'chat' in post_type:
                data_1 = js['posts'][post]['title']
                data_2 = js['posts'][post]['body']

        except KeyError:
            print("Couldn't find details for {} post!".format(post_type))
            print(js['posts'][post])
            data_1 = None
            data_2 = None

        db.write(post_type, post_id, date, notes, tags, data_1, data_2)

    if post_count or post_count >= 20:
        return True
    else:
        db.commit()


def mkdir_output():
    """Make local output folder"""
    try:
        os.mkdir('output')
        print(f'\n[+] Creating output folder')
    except FileExistsError:
        pass
    except PermissionError:
        raise SystemExit(f'\n[!] Can\'t create directory "output"\n')
    os.chdir('output')


class Database:

    def __init__(self, blog):
        """Initialize local database while checking for an existing one

        :param blog: Tumblr blog name

        """
        db = f'{blog}.db'
        self.check_backup(db)

        try:
            self.conn = sqlite3.connect(db)
            print(f'[+] Creating new DB "{db}"\n')
        except sqlite3.OperationalError:
            raise SystemExit(f'\n[!] Can\'t create DB {db}\n')

        self.cur = self.conn.cursor()
        self.create_tables()

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
                                               img TEXT,
                                               caption TEXT)""")

        self.cur.execute("""CREATE TABLE photoset(post_id INT,
                                                  date TEXT,
                                                  notes INT,
                                                  tags TEXT,
                                                  photoset TEXT,
                                                  caption TEXT)""")

        self.cur.execute("""CREATE TABLE text(post_id INT,
                                              date TEXT,
                                              notes INT,
                                              tags TEXT,
                                              title TEXT,
                                              body TEXT)""")

        self.cur.execute("""CREATE TABLE answer(post_id INT,
                                                date TEXT,
                                                notes INT,
                                                tags TEXT,
                                                question TEXT,
                                                answer TEXT)""")

        self.cur.execute("""CREATE TABLE video(post_id INT,
                                               date TEXT,
                                               notes INT,
                                               tags TEXT,
                                               video_url TEXT,
                                               caption TEXT)""")

        self.cur.execute("""CREATE TABLE quote(post_id INT,
                                               date TEXT,
                                               notes INT,
                                               tags TEXT,
                                               quote TEXT,
                                               source TEXT)""")

        self.cur.execute("""CREATE TABLE link(post_id INT,
                                              date TEXT,
                                              notes INT,
                                              tags TEXT,
                                              title TEXT,
                                              url TEXT)""")

        self.cur.execute("""CREATE TABLE audio(post_id INT,
                                               date TEXT,
                                               notes INT,
                                               tags TEXT,
                                               url TEXT,
                                               caption TEXT)""")

        self.cur.execute("""CREATE TABLE chat(post_id INT,
                                              date TEXT,
                                              notes INT,
                                              tags TEXT,
                                              title TEXT,
                                              body TEXT)""")

    def write(self, post_type, post_id, date, notes, tags, data_1, data_2):
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
        self.cur.execute(f'INSERT INTO {post_type} VALUES(?, ?, ?, ?, ?, ?)',
                         (post_id, date, notes, tags, data_1, data_2))

    def commit(self):
        """Commmit everything and close out the DB

        We *could* commit after every write, in self.write() above,
        but that's really slow.

        """
        self.conn.commit()
        self.conn.close()


if __name__ == '__main__':

    main()
