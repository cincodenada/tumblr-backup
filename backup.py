import argparse
import os
import sqlite3
from time import sleep

import requests

# PATH to file with api key
CONFIG = '../config.txt'

# Be nice to tumblr servers
RATE_LIMIT = 4  # seconds


def main():
    """Main logic method"""
    cur_path = os.path.dirname(os.path.abspath(__file__))
    os.chdir(cur_path)

    args = parse_args()
    blog = args.blog
    offset = args.offset

    api_key = parse_api_key()
    url = f'https://api.tumblr.com/v2/blog/{blog}.tumblr.com/posts/?api_key={api_key}'

    mkdir_output()
    db = Database(blog)

    while True:
        js = get_json(f'{url}&offset={offset}&limit=20')
        if not remaining_json(js, db):
            return print('\nFinished\n')
        else:
            offset += 20
            print(f'\noffset {offset}\n')
            sleep(RATE_LIMIT)


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
    try:
        with open(CONFIG) as file:
            api_key = file.readline().strip()
            return api_key
    except FileNotFoundError:
        raise SystemExit(f'\n[!] Config file missing "{CONFIG}"\n')


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

        if 'photo' in post_type:
            caption = js['posts'][post]['caption']
            photo_count = len(js['posts'][post]['photos'])

            if photo_count > 1:
                # photoset
                photos = []
                for photo in range(photo_count):
                    img = js['posts'][post]['photos'][photo]['original_size']['url']
                    photos.append(img)
                photoset = ','.join(photos)  # convert list to string for db
                db.write('photoset', post_id, date, notes, tags, photoset, caption)
            else:
                # photo
                img = js['posts'][post]['photos'][0]['original_size']['url']
                db.write(post_type, post_id, date, notes, tags, img, caption)

        elif 'video' in post_type:
            caption = js['posts'][post]['caption']
            video_type = js['posts'][post]['video_type']

            if 'instagram' in video_type:
                video_url = js['posts'][post]['permalink_url']
            elif 'tumblr' in video_type:
                video_url = js['posts'][post]['video_url']
            elif 'youtube' in video_type:
                video_id = js['posts'][post]['video']['youtube']['video_id']
                video_url = f'https://www.youtube.com/watch?v={video_id}'
            else:
                video_url = None
            db.write(post_type, post_id, date, notes, tags, video_url, caption)

        elif 'answer' in post_type:
            question = js['posts'][post]['question']
            answer = js['posts'][post]['answer']
            db.write(post_type, post_id, date, notes, tags, question, answer)

        elif 'text' in post_type:
            title = js['posts'][post]['title']
            body = js['posts'][post]['body']
            db.write(post_type, post_id, date, notes, tags, title, body)

        elif 'quote' in post_type:
            quote = js['posts'][post]['text']
            source = js['posts'][post]['source']
            db.write(post_type, post_id, date, notes, tags, quote, source)

        elif 'link' in post_type:
            title = js['posts'][post]['title']
            url = js['posts'][post]['url']
            db.write(post_type, post_id, date, notes, tags, title, url)

        elif 'audio' in post_type:
            caption = js['posts'][post]['caption']
            url = js['posts'][post]['source_url']
            db.write(post_type, post_id, date, notes, tags, url, caption)

        elif 'chat' in post_type:
            title = js['posts'][post]['title']
            body = js['posts'][post]['body']
            db.write(post_type, post_id, date, notes, tags, title, body)

    if post_count or post_count >= 20:
        return True
    else:
        db.commit()


def mkdir_output():
    """Make local output folder"""
    try:
        print(f'\n[+] Creating output folder')
        os.mkdir(f'output')
    except FileExistsError:
        pass
    except PermissionError as e:
        raise SystemExit(f'[!] Can\'t create directory "output"')
    os.chdir(f'output')


class Database:

    def __init__(self, blog):
        """Initialize local database while checking for an existing one

        :param blog: Tumblr blog name

        """
        db = f'{blog}.db'
        if os.path.isfile(db):
            print(f'\n[~] Found existing DB "{db}"')
            print(f'[~] Making backup "{db}.bak"\n')
            os.rename(db, f'{db}.bak')

        self.conn = sqlite3.connect(db)
        self.cur = self.conn.cursor()
        self.create_tables()

    def create_tables(self):
        """Create DB tables"""
        self.cur.execute("""CREATE TABLE IF NOT EXISTS photo(post_id INT,
                                                             date TEXT,
                                                             notes INT,
                                                             tags TEXT,
                                                             img TEXT,
                                                             caption TEXT)""")
        self.cur.execute("""CREATE TABLE IF NOT EXISTS photoset(post_id INT,
                                                                date TEXT,
                                                                notes INT,
                                                                tags TEXT,
                                                                photoset TEXT,
                                                                caption TEXT)""")
        self.cur.execute("""CREATE TABLE IF NOT EXISTS text(post_id INT,
                                                            date TEXT,
                                                            notes INT,
                                                            tags TEXT,
                                                            title TEXT,
                                                            body TEXT)""")
        self.cur.execute("""CREATE TABLE IF NOT EXISTS answer(post_id INT,
                                                              date TEXT,
                                                              notes INT,
                                                              tags TEXT,
                                                              question TEXT,
                                                              answer TEXT)""")
        self.cur.execute("""CREATE TABLE IF NOT EXISTS video(post_id INT,
                                                             date TEXT,
                                                             notes INT,
                                                             tags TEXT,
                                                             video_url TEXT,
                                                             caption TEXT)""")
        self.cur.execute("""CREATE TABLE IF NOT EXISTS quote(post_id INT,
                                                             date TEXT,
                                                             notes INT,
                                                             tags TEXT,
                                                             quote TEXT,
                                                             source TEXT)""")
        self.cur.execute("""CREATE TABLE IF NOT EXISTS link(post_id INT,
                                                             date TEXT,
                                                             notes INT,
                                                             tags TEXT,
                                                             title TEXT,
                                                             url TEXT)""")
        self.cur.execute("""CREATE TABLE IF NOT EXISTS audio(post_id INT,
                                                             date TEXT,
                                                             notes INT,
                                                             tags TEXT,
                                                             url TEXT,
                                                             caption TEXT)""")
        self.cur.execute("""CREATE TABLE IF NOT EXISTS chat(post_id INT,
                                                             date TEXT,
                                                             notes INT,
                                                             tags TEXT,
                                                             title TEXT,
                                                             body TEXT)""")

    def write(self, post_type, post_id, date, notes, tags, *args):
        """Each post type will have 2 additional values for the db (as *args)

        photo: img, caption
        photoset: photoset, caption
        video: video_url, caption
        answer: question, answer
        text: title, body
        quote: quote, source
        link: title, url
        audio: url, caption
        chat: title, body

        These 2 values are args[0] and args[1] in the db entry

        """
        print(f'{post_type} - #{post_id}')
        self.cur.execute(f'INSERT INTO {post_type} VALUES(?, ?, ?, ?, ?, ?)',
                         (post_id, date, notes, tags, args[0], args[1]))

    def commit(self):
        """Commmit everything and close out the DB

        We *could* commit after every write, in self.write() above, but that's
        really slow.

        """
        self.conn.commit()
        self.conn.close()


if __name__ == '__main__':

    main()
