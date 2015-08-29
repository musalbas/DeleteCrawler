from HTMLParser import HTMLParser
import re
import sqlite3 as sqlite
import sys
import urllib2


"""Bot to crawl Twitter and update the database.

sqlitecon: a sqlite connection object."""
class DeleteCrawlerBot(object):
    sql_create_username_table = 'CREATE TABLE IF NOT EXISTS usernames(username TEXT PRIMARY KEY NOT NULL);'
    sql_create_tweets_table = 'CREATE TABLE IF NOT EXISTS tweets(username TEXT NOT NULL, tweet TEXT NOT NULL, rawtweet TEXT NOT NULL, tweetid INTEGER UNIQUE NOT NULL, deleted INTEGER DEFAULT 0, retweet_of TEXT DEFAULT NULL);'

    sql_insert_username = 'INSERT INTO usernames VALUES(?);'
    sql_insert_tweet = 'INSERT INTO tweets VALUES(?, ?, ?, ?, 0, ?)'

    sql_select_tweets_by_username_tweetid = 'SELECT * FROM tweets WHERE username = ? AND tweetid >= ?'

    sql_update_tweet_deleted = 'UPDATE tweets SET deleted = 1 WHERE tweetid = ?'

    def __init__(self, sqlitecon):
        self.sqlitecon = sqlitecon
        self.sqlitecur = sqlitecon.cursor()

        self._init_db()

    def _init_db(self):
        self.sqlitecur.execute(self.sql_create_username_table)
        self.sqlitecur.execute(self.sql_create_tweets_table)

    """Add username for tweets tracking.

    username: Twitter username.
    """
    def add_username(self, username):
        self.sqlitecur.execute(self.sql_insert_username, (username,))

    """Crawl an account and update the database.

    username: Twitter username.
    pages: number of pages to crawl."""
    def crawl_account(self, username, pages):
        crawler = TwitterAccountCrawler(username)
        tweets = []
        for i in range(pages):
            tweets += crawler.get_next_page()

        last_tweetid = None
        for i in range(len(tweets)):
            if tweets[-(i+1)][4] is None:
                last_tweetid = tweets[-(i+1)][3]
        if last_tweetid is None:
            return # cannot determine last non-RT tweetid

        db_tweetids = []
        for tweet in self.sqlitecur.execute(self.sql_select_tweets_by_username_tweetid, (username, last_tweetid)).fetchall():
            db_tweetids.append(tweet[3])

        source_tweetids = []
        for tweet in tweets:
            source_tweetids.append(tweet[3])
            if tweet[3] not in db_tweetids:
                try:
                    self.sqlitecur.execute(self.sql_insert_tweet, tweet)
                except sqlite.IntegrityError:
                    # no worries, this is an RT older than the time period of the tweetids range
                    pass

        for db_tweetid in db_tweetids:
            if db_tweetid not in source_tweetids:
                # tweet deleted!
                self.sqlitecur.execute(self.sql_update_tweet_deleted, (db_tweetid,))


"""A crawler to get tweets from a Twitter account.

username: the username of the Twitter account to crawl.
"""
class TwitterAccountCrawler(object):
    re_tweetid = re.compile('<div class="tweet-text" data-id="([0-9]+)">')
    re_max_id = re.compile('max_id=([0-9]+)">Load older Tweets</a>')

    def __init__(self, username):
        self.username = username

        self._htmlparser = HTMLParser()
        self._current_max_id = None

    """Get the next page of tweets."""
    def get_next_page(self):
        url = 'https://mobile.twitter.com/' + self.username
        if self._current_max_id is not None:
            url += '?max_id=' + self._current_max_id

        html = urllib2.urlopen(url).read()
        return self._get_tweets_from_html(html)

    def _get_tweets_from_html(self, html):
        current_username = None
        current_tweetid = None
        current_tweet = None
        current_rawtweet = None
        tweets = []

        for line in html.splitlines():
            if '<span>@</span>' in line:
                current_username = line.replace('<span>@</span>', '').strip()
            elif '<div class="tweet-text"' in line:
                current_tweetid = self.re_tweetid.search(line).group(1)
                current_tweetid = int(current_tweetid)
            elif '<div class="dir-ltr" dir="ltr">  ' in line:
                current_rawtweet = line.replace('<div class="dir-ltr" dir="ltr">', '').strip()
                current_tweet = re.sub('<[^<]+?>', '', current_rawtweet)
                current_tweet = self._htmlparser.unescape(current_tweet.decode('utf8')).strip()
                if self.username != current_username:
                    current_retweet_of = current_username
                else:
                    current_retweet_of = None
                tweets.append((self.username, current_tweet, current_rawtweet, current_tweetid, current_retweet_of))
            elif 'max_id' in line:
                self._current_max_id = self.re_max_id.search(line).group(1)

        return tweets

if __name__ == '__main__':
    try:
        dbpath = sys.argv[1]
    except IndexError:
        dbpath = 'deletecrawler.db'

    sqlitecon = sqlite.connect(dbpath)
    deletecrawlerbot = DeleteCrawlerBot(sqlitecon)

    sqlitecon.commit()
    sqlitecon.close()
