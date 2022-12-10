import configparser
import os
import sys
import traceback 
import argparse

from twitchApi import TwitchApi

DIRPATH = os.path.dirname(os.path.realpath(__file__))
CONFIGFILE = os.path.join(DIRPATH, "config.ini")
DATABASEFILE = os.path.join(DIRPATH, "clips.sqlite3")

twitchApi: TwitchApi = None
config = {}

if os.path.exists(CONFIGFILE):
  print(f"local config file exists")
  print(f"loading configs from {CONFIGFILE}")
  try:
    config = configparser.ConfigParser()
    config.read(CONFIGFILE, encoding='utf8')
    config = config['settings']
  except Exception as e:
    print(f"config parse error while reading {CONFIGFILE}")
    print(e)


def init_twitchApi(argDatabase, argClientId, argClientSecret, argStreamer, argReadSize, argProxy):
  global config, twitchApi
  databaseFile = argDatabase if argDatabase != None else DATABASEFILE
  clientId = argClientId if argClientId != None else config.get('clientId', None)
  clientSecret = argClientSecret if argClientSecret != None else config.get('clientSecret', None)
  streamerId = argStreamer if argStreamer != None else config.get('streamerId', None)
  readSize = argReadSize if argReadSize != None else config.get('readSize', 40)
  proxy = argProxy if argProxy != None else config.get('proxy', None)
  
  try:
    readSize = int(readSize)
    if readSize < 1:
      readSize = 1
    elif readSize > 100:
      readSize = 100
  except:
    readSize = 40
  
  if databaseFile != None and len(databaseFile) == 0:
    raise Exception("database file path is not valid")
  if proxy != None and len(proxy) == 0:
    proxy = None
  if clientId == None or len(clientId) == 0:
    raise Exception("client_id is needed")
  if clientSecret == None or len(clientSecret) == 0:
    raise Exception("clientSecret is needed")
  if streamerId == None or len(streamerId) == 0:
    raise Exception("streamer_id is needed")
  print(f'''
    Init parameters
      databaseFile  {os.path.realpath(databaseFile)}
      clientId      HIDDEN
      clientSecret  HIDDEN
      streamerId    {streamerId}
      readSize      {readSize}
      proxy         {'HIDDEN' if proxy != None else 'NOT SET'}
  ''')
  twitchApi = TwitchApi(databaseFile, clientId, clientSecret, streamerId, readSize, proxy)


def write_json(argDownloadDirectory, argConcurrency):
  global twitchApi
  try:
    downloadDirectory = argDownloadDirectory if argDownloadDirectory != None else config.get('downloadDirectory', None)
    concurrency = argConcurrency if argConcurrency != None else config.get('concurrency', 6)
    if downloadDirectory == None:
      raise Exception(f"download directory is not specified!")
    try:
      concurrency = int(argConcurrency)
    except:
      concurrency = 6
    
    if concurrency < 0:
      concurrency = 1
      
    print(f'''
    write_json parameters
      downloadDirectory   {os.path.realpath(downloadDirectory)}
      concurrency         {concurrency}
    ''')
    twitchApi.write_json_from_database(downloadDirectory, concurrency)
  except Exception as e:
    traceback.print_exception(e)
    sys.exit(1)
  sys.exit(0)


def make_database(argFromDatabaseDate=False):
  global twitchApi
  try:
    fromDatabaseDate = argFromDatabaseDate if argFromDatabaseDate != None else config.get('fromDatabaseDate', False)
    
    print(f'''
    Read clips parameters
      fromDatabaseDate   {fromDatabaseDate}
    ''')
    twitchApi.read_all_clips((fromDatabaseDate == True))
  except Exception as e:
    traceback.print_exception(e)
    sys.exit(1)


def download_clips_from_database(argDownloadDirectory, argConcurrency, argSaveJson, argForceDownload, argMinView, argMaxClips):
  global config, twitchApi
  try:
    downloadDirectory = argDownloadDirectory if argDownloadDirectory != None else config.get('downloadDirectory', None)
    saveJson = argSaveJson if argSaveJson != None else config.get('saveJson', False)
    forceDownload = argForceDownload if argForceDownload != None else config.get('forceDownload', False)
    minView = argMinView if argMinView != None else config.get('minView', -1)
    maxClips = argMaxClips if argMaxClips != None else config.get('maxClips', -1)
    concurrency = argConcurrency if argConcurrency != None else config.get('concurrency', 6)
    
    if downloadDirectory == None:
      raise Exception(f"download directory is not specified!")
    
    try:
      concurrency = int(argConcurrency)
    except:
      concurrency = 6
    
    if concurrency < 0:
      concurrency = 1
      
    try:
      minView = int(minView)
    except:
      minView = 0
    if minView < 0:
      minView = 0
    
    try:
      maxClips = int(maxClips)
    except: 
      maxClips = -1 
    if maxClips <= 0:
      maxClips = -1
    
    print(f'''
    Download parameters
      downloadDirectory   {os.path.realpath(downloadDirectory)}
      saveJson            {saveJson}
      forceDownload       {forceDownload}
      minView             {minView}
      maxClips            {maxClips}
      concurrency         {concurrency}
    ''')
    twitchApi.download_clips_from_database(downloadDirectory, concurrency, saveJson, forceDownload, minView, maxClips)
  except Exception as e:
    traceback.print_exception(e)
    sys.exit(1)


if __name__ == "__main__":
  parser = argparse.ArgumentParser(
    prog="Twitch Clip Archiver",
    description="Read all clips and save to database. Download clips if needed.",
    epilog="It reads `config.ini` first then apply arguments if passed.",
  )
  
  parser.add_argument("-n", "--skip-build-database", action="store_true", help="use existing database without requesting from server")
  parser.add_argument("-d", "--download", action="store_true", help="download all clips in database")
  parser.add_argument("-j", "--save-json", action="store_true", help="save clip information as json file")
  parser.add_argument("-f", "--force-download", action="store_true", help="re-download file if marked as downloaded")
  parser.add_argument("-z", "--from-database-date", action="store_true", help="read clips from twitch in range from the latest month in database")
  
  parser.add_argument("--json-only", action="store_true", help="update json file from database information. Use with download_directory option")
  
  parser.add_argument("--client-id", help="twitch client id")
  parser.add_argument("--client-secret", help="twitch client secret")
  
  parser.add_argument("-b", "--database", help="database path")
  parser.add_argument("-s", "--streamer", help="streamer loginName(not nickname!!!) or broadcaster_id(number string)")
  parser.add_argument("-o", "--download-directory", help="path to save clips")
  parser.add_argument("-m", "--min-view", help="minimum view count to download (default=0)")
  parser.add_argument("-M", "--max-clips", help="maximun number of clips to download. -1 is infinite. (default=-1)")
  parser.add_argument("--read-size", help="the number of clips fetch from twitch server. (default=40)")
  parser.add_argument("--concurrency", help="download concurrency. (default=6)")
  parser.add_argument("--proxy", help="proxy url")
  
  args = parser.parse_args() 
  
  init_twitchApi(args.database, args.client_id, args.client_secret, args.streamer, args.read_size, args.proxy)
  
  if args.skip_build_database != True:
    print(f"Read clips from twitch server...")
    make_database(
      (args.from_database_date == True)
    )
  
  if args.json_only == True:
    # exits program
    print(f"Overwrite all json files")
    write_json(
      args.download_directory,
      args.concurrency,
    )
    
  if args.download == True:
    print(f"Download clips...")
    download_clips_from_database(
      args.download_directory, 
      args.concurrency,
      (args.save_json == True),
      (args.force_download == True),
      args.min_view,
      args.max_clips,
    )
  
