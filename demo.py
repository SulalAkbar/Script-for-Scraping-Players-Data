from fake_useragent import UserAgent
from requests.exceptions import HTTPError
from requests import ReadTimeout, ConnectTimeout, HTTPError, Timeout, ConnectionError,TooManyRedirects
from bs4 import BeautifulSoup
import random
import csv
import requests
import time
from lxml import html
import sqlite3
from sqlite3 import Error
import json

db_file = 'players.db'
base_url = ''

def csv_writer(_list,file):
    with open(file, 'a', newline="") as csv_file:
        writer = csv.writer(csv_file)
        for team in _list:
            writer.writerow([team])
    csv_file.close()


def csv_reader(file):
    with open(file) as csv_file:
        reader = csv.reader(csv_file)
        for row in reader:
            proxies.append(row.pop())

proxies = []
csv_reader('proxy-list.csv')
proxy_length = len(proxies)
def random_proxy():
  return random.randint(0, proxy_length - 1)


proxy_index = random_proxy()
proxy = proxies[proxy_index]

ua = UserAgent()

### Function for fetching web page

def get_url(url,proxy):
    pageContent = 0
    retry = 10
    access = False
    while retry > 0 and access == False:
        print('In Loop-->')
        try:
            print('I am in Try')
            response = requests.get(url,headers={'User-Agent':ua.random},timeout=10,proxies={"http":proxy, "https":proxy})
            print('Got Page')
            pageContent = response
            time.sleep(5)
            access = True
            retry= -1
            break
        except (ConnectTimeout, HTTPError, ReadTimeout, Timeout, ConnectionError ,TooManyRedirects) as e:
            proxy_index = random_proxy()
            proxy = proxies[proxy_index]
            print('Exception','--> ',e,'--','New Proxy Index -->',proxy_index)
            retry = retry-1
            print('Sleeping')
            time.sleep(5)
            print('Awaked')

    return pageContent


################################################
#Data Base Functions
def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn


def create_team(team_name):
    conn=create_connection(db_file)

    if conn is not None:
        sql = ''' INSERT INTO teams(team_id,team_name) VALUES (?,?) '''

        try:
            cur = conn.cursor()
            cur.execute(sql,team_name)
        except:
            print('Error in inserting team name')
    else:
        print('Error in database (create team)')

    team_id = cur.lastrowid
    conn.commit()
    conn.close()
    return team_id

def add_player(player_record):
    conn=create_connection(db_file)

    if conn is not None:
        sql = ''' INSERT INTO players(player_id,player_name,player_history,team_id) VALUES (?,?,?,?) '''

        try:
            cur = conn.cursor()
            cur.execute(sql,player_record)
        except Exception as e:
            print('Error in inserting Player Record',e)
    else:
        print('Error in database (create team)')

    team_id = cur.lastrowid
    conn.commit()
    conn.close()
    return team_id
#####################
#Function for Scraping Club Players Name and Url
def get_url_name_of_players(r):
    soup = BeautifulSoup(r.text, 'html.parser')

    tr = soup.find_all('tr',{ "class" : "odd"})

    tr2 = soup.find_all('tr',{ "class" : "even"})

    players_table = tr+tr2

    url_and_name_of_team_players = []
    for t in players_table:
        player=t.findChild()
        dict = {'name':player.text,'url':player['href']}
        url_and_name_of_team_players.append(dict)

    return url_and_name_of_team_players

###############################################
##Function for Scraping Players Transfer History from Players Page
def player_history_scraper(r):
    soup = BeautifulSoup(r.text, 'html.parser')

    tr = soup.find_all()

    complete_scraped_list = []
    for i in tr:
        a=i.findChild()
        if a==None:
            continue
        else:
            complete_scraped_list.append(a)
    temp = []
    player_history_list = []
    for a in complete_scraped_list:
        if a.text in temp:
            pass
        else:
            player_history_list.append(a.text)
            temp.append(a.text)

    return player_history_list
################################################
print('Getting Leagues Page')


response = get_url('',proxy)

list_of_teams = []
if response == 0:
    print('Sorry Boy')
else:
    tree = html.fromstring(response.content)
    for i in range(18):
        club_link=tree.xpath('')
        club_name=tree.xpath('')

        team_id = (None,club_name) # Inserting Team Name , return value will be team id

        print('Getting Club Page ..',i)
        r=get_url(base_url+club_link[0])
        print('Got Club Page',i)

        print('Scraping Urls And Names of Players of  Club -->',i)
        player_urls=get_url_name_of_players(r)
        print('Scraped Urls And Names of Club of Players of Club  -->',i)

        count = 0
        for player in player_urls:

            print('Getting Player Histroy Page ->',count)
            r=get_url(base_url+player['url'])
            print('Got Player Histroy Page ->',count)

            list_of_transfer=player_history_scraper(r)
            print('Scraped Player Histroy Page')

            print('Saving in Database')
            p1 = (None,player['name'],json.dumps(list_of_transfer),team_id)

            add_player(p1)
            print('Player info saved ->',count)

            count = count+1
    print('Got All Data Now Check Data Base')

