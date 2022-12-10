import os 
import sys
import requests 
import json
import time
from datetime import datetime, timezone
import subprocess

from tqdm import tqdm

from database import ClipDatabase


def replace_invalid_filename(source):
    replace_list = {
      ':': '%3A',
      '/': '%2F',
      '\\': '%5C',
      '*': '%2A',
      '?': '%3F',
      '"': "%22",
      '<': '%3C',
      '>': '%3E',
      '|': '%7C',
      '\n': '',
      '\r': '',
    }
    for key in replace_list.keys():
      source = source.replace(key, replace_list[key])
    return source

def truncate_string_in_byte_size(unicode_string, size=180):
  if len(unicode_string.encode('utf8')) > size:
    return unicode_string.encode('utf8')[:size].decode('utf8', 'ignore').strip() + '...'
  return unicode_string

class TwitchApi:
  def __init__(self, databasePath: str, clientId: str, clientSecret: str, streamerId: str, readSize: int, proxy: str):
    self.database = ClipDatabase(databasePath)
    self.session = requests.Session()
    self.authHeader = {}
     
    self.clientId = clientId
    self.clientSecret = clientSecret
    self.proxy = proxy
    self.readSize = readSize
    
    self.proxies = {
      "http": proxy,
      "https": proxy,
    }   
    
    self.__get_credentials()
    self.broadcasterId = streamerId if self.__is_broadcaster_id(streamerId) else self.__get_broadcaster_id(streamerId)
    self.loginName = streamerId if not self.__is_broadcaster_id(streamerId) else self.__get_loginName(streamerId)
    self.database.create_table(self.loginName)
  

  def __get(self, url, headers={}):
    headers.update(self.authHeader)
    if self.proxy == None:
      res = self.session.get(url, headers=headers)
    else:
      res = self.session.get(url, headers=headers, proxies=self.proxies)
    if not res.ok:
      raise Exception(res.json())
    return res.json()
  
  def __post(self, url, headers={}, data=None, json=None):
    if self.proxy == None:
      res = self.session.post(url, headers=headers, data=data, json=json)
    else:
      res = self.session.post(url, headers=headers, data=data, json=json, proxies=self.proxies)
    if not res.ok:
      raise Exception(res.json())
    return res.json()
  
  def __is_broadcaster_id(self, name):
    try:
      int(name)
      return True
    except:
      return False 
  
  def __get_broadcaster_id(self, loginName):
    try:
      api = f"https://api.twitch.tv/helix/users?login={loginName}"
      res = self.__get(api)
      return res['data'][0]['id']
    except Exception as e:
      print(e)
      print(f"{loginName} is not valid or credentials is not valid")
      raise Exception(e)

  def __get_loginName(self, boradcasterId):
    try:
      api = f"https://api.twitch.tv/helix/users?id={boradcasterId}"
      res = self.__get(api)
      return res['data'][0]['login']
    except Exception as e:
      print(e)
      print(f"{boradcasterId} is not valid or credentials is not valid")
      raise Exception(e)

  def __get_credentials(self):
    try:
      api = f"https://id.twitch.tv/oauth2/token?grant_type=client_credentials"
      res = self.__post(
        api,
        headers={
          "Content-Type":"application/x-www-form-urlencoded",
        },
        data={
          'client_id': self.clientId,
          'client_secret': self.clientSecret,
          'redirect_uri': 'localhost',
          'code': self.clientSecret,
        }
      )
      token = f"Bearer {res['access_token']}"
      self.authHeader = {
        'Authorization': token,
        'Client-Id': self.clientId,
      }
      return token
    except Exception as e:
      print(e)
      print(f"credentials is not valid")
      raise Exception(e)
    
  
  
  def read_clips(self, after, started_at, ended_at):    
    api = f"https://api.twitch.tv/helix/clips?broadcaster_id={self.broadcasterId}&first={self.readSize}"
    if after != None and len(after) != 0:
      api += f"&after={after}"
    if started_at != None and len(started_at) != 0:
      api += f"&started_at={started_at}"
    if ended_at != None and len(ended_at) != 0:
      api += f"&ended_at={ended_at}"
    return self.__get(api)

  
  def read_all_clips(self, from_database_date: bool):
    """_summary_
    클립 기능의 최초 도입 날짜는 2016-05-26T00:00:00Z임
    started_at과 ended_at을 명시하지 않고 조회하면
    클립 조회수 내림차순으로 목록을 리턴하지만 
    최대 100,000개까지 밖에 조회할 수 없음.
    
    그러므로
    2016-01-01T00:00:00Z ~ 2017-01-01T00:00:00Z
    2017-01-01T00:00:00Z ~ 2018-01-01T00:00:00Z
    위와 같이 범위를 지정해주면
    모든 클립을 다 조회할 수 있음.
    
    년에 100,000개 이하의 클립이 있다는 가정이 맞다면
    위와 같이 범위를 지정하면 되고
    더 안전하게 하려면 월 단위로 나누어서 쿼리를 
    보내면 될 듯.
    
    TODO
    fromDatabaseDate == True이면 database로부터 
    가장 최신의 created_at을 가져와서
    그 범위부터 요청함.
    
    Raises:
        KeyboardInterrupt: _description_
    """
    def date_range_generator(start_year: int, start_month: int):
      today = datetime.now()
      today_year = today.year 
      today_month = today.month 
      
      year = start_year 
      month = start_month
      while True:
        if f'{year}-{str(month).zfill(2)}' > f'{today_year}-{str(today_month).zfill(2)}':
          break
        started_at = f"{year}-{str(month).zfill(2)}-01T00:00:00Z"
        if month == 12:
          month = 1 
          year += 1
        else:
          month += 1
        # 마지막 날 5분으로 설정해서 누락되는 클립 없는가 확인
        ended_at = f"{year}-{str(month).zfill(2)}-01T00:05:00Z" 
        yield (started_at, ended_at)
    
    def expand_clip(clip: dict):
      clip['vod_url'] = clip['thumbnail_url'][:(clip['thumbnail_url'].index('-preview-'))] + '.mp4'
      clip['updated_at'] = datetime.now()
      return clip 
    
    start_year, start_month = 2016, 1
    if from_database_date:
      (start_year, start_month) = self.database.get_latest_created_at(self.loginName)
    
    num_of_clips = 0
    with tqdm(unit='clip') as progress_bar:
      for (started_at, ended_at) in date_range_generator(start_year, start_month):
        progress_bar.set_description_str(f"[{started_at} ~ {ended_at}]")
        clips = {}
        after = ""
        tries = 0
        while tries < 3:
          try:
            res_json = self.read_clips(after, started_at, ended_at)
            clips = res_json['data']
            clips = [expand_clip(clip) for clip in clips]
            pagination = res_json['pagination']
            num_of_clips += len(clips)
            if len(clips) > 0:
              self.database.insertmany_item(self.loginName, clips)
              progress_bar.update(len(clips))
            if 'cursor' not in pagination:
              break
            after = pagination['cursor']
          except KeyboardInterrupt:
            raise KeyboardInterrupt
          except Exception as e:
            print(f"\n[{datetime.now()}] {tries+1}-th try {e}")
            tries += 1
        if tries >= 3:
          print(f"\n[{datetime.now()}] Failed while requesting ({after}, {started_at}, {ended_at}) => {clips}", flush=True)
    print(f"total clips with duplicated: {num_of_clips}")


  def path_constructor(self, downloadDirectory: str, clip: dict):
    """ 
    make parent directories and 
    returns full-path-without-file-extension
    """
    
    
    """ 
    '2017-12-29T13:12:23Z' -> '2017-12-29T13:12:23'
    """
    created_at = datetime.fromisoformat(clip['created_at'][:-1]) + datetime.now(timezone.utc).astimezone().utcoffset()
    year = str(created_at.year).zfill(4)
    month = str(created_at.month).zfill(2)
    day = str(created_at.day).zfill(2)
    hour = str(created_at.hour).zfill(2)
    minute = str(created_at.minute).zfill(2)
    second = str(created_at.second).zfill(2)
    
    broadcasterDirectory = f"{clip['broadcaster_name']} ({self.loginName})"
    clip_title = truncate_string_in_byte_size(clip['title'].strip())
    clip_id = clip["id"][:10]
    title = f"[{year}{month}{day}-{hour}{minute}{second}] {clip_title} ({clip_id})"
    title = replace_invalid_filename(title)
    fileDirectory = os.path.join(
      downloadDirectory, 
      broadcasterDirectory, 
      year,
      f"{year}-{month}",
      f"{year}-{month}-{day}",
    )
    os.makedirs(fileDirectory, exist_ok=True)
    return os.path.join(fileDirectory, title)


  def save_json(self, clip: dict, filename: str):
    try:
      json_data = clip.copy()
      json_data.pop('_id', None)
      json_data.pop('download_status', None)
      json_data.pop('download_path', None)
      with open(filename, 'w', encoding="utf-8") as f:
        json.dump(json_data, f, indent=2, ensure_ascii=False)
      return (True, clip) 
    except Exception as e:
      return (False, clip)
    

  def download_clip(self, clip: dict, downloadDirectory: str, saveJson: bool) -> dict:
    def streamlink_method(commands: list):
      try:
        completed_process = subprocess.run(
          commands,
          capture_output=True
        )
        return_code = completed_process.returncode
        return (return_code == 0)
      except Exception as e:
        # print(f"streamlink_method failed | {e}", flush=True)
        return False 
    
    def request_method(vod_url, filename, proxy={}):
      try:
        res = requests.get(
          vod_url, 
          stream=True, 
          proxies=proxy
        ) 
        if not res.ok:
          return False 
        with open(filename, 'wb') as f: 
          for chunk in res.iter_content(chunk_size=1024*1024): 
            if chunk:
              f.write(chunk)
        return True
      except Exception as e:
        # print(f"request_method failed | {e}", flush=True)
        return False 
    
    filename = self.path_constructor(downloadDirectory, clip)
    clip_path = f'{filename}.mp4' # json 저장 때문에 다른 변수 사용함
    
    # set status as pending
    clip['download_status'] = 2
    clip['download_path'] = os.path.realpath(clip_path)
    
    success = False 
    
    proxy_option = [] if self.proxy == None else ["--http-proxy", self.proxy]
    commands = [sys.executable, "-m", "streamlink", "-o", clip_path, "--force"] + proxy_option + [clip["url"], "best"]
    for _ in range(2):
      success = streamlink_method(commands)
      if success: 
        break 
      time.sleep(2) 
    
    if not success:
      print(f"\n[{datetime.now()}] Use request method for {clip['created_at']}-{clip['url']}", flush=True)
      for _ in range(2):
        success = request_method(clip['vod_url'], clip_path, self.proxies)
        if success: 
          break 
        time.sleep(2)
    
    if not success:
      print(f"\n[{datetime.now()}] Failed to download {clip['created_at']}-{clip['url']}", flush=True)
      return clip 

    if saveJson == True:
      self.save_json(clip, f'{filename}.json')

    # set as downloaded
    clip['download_status'] = 1
    return clip


  def download_clips_from_database(self, downloadDirectory: str, concurrency: int, saveJson: bool, forceDownload: bool, minView: int, maxClips: int):
    def clip_handler(clip):
      return self.download_clip(clip, downloadDirectory, saveJson)
    self.database.iterate_incomplete_rows(
      self.loginName, 
      clip_handler, 
      concurrency, 
      minView, 
      maxClips, 
      forceDownload
    )
  
  
  def write_json_from_database(self, downloadDirectory: str, concurrency: int):
    def save_json_clip_handler(clip):
      filename = self.path_constructor(downloadDirectory, clip)
      return self.save_json(clip, f'{filename}.json')
    
    self.database.iterate_completed_rows(
      self.loginName, 
      save_json_clip_handler,
      concurrency,
    )
    
  
