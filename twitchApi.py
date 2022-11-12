import os 
import sys
import requests 
import json
import time
from datetime import datetime, timezone, timedelta
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
  def __init__(self, databasePath: str, clientId: str, clientSecret: str, streamerId: str, readSize: str, proxy: str):
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
  
  def __post(self, url, headers={}, data={}):
    if self.proxy == None:
      res = self.session.post(url, headers=headers, data=data)
    else:
      res = self.session.post(url, headers=headers, data=data, proxies=self.proxies)
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

  
  def read_all_clips(self):
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
    db로부터 마지막 created_at을 가져온 후
    그 날짜 이후로부터 요청하면 효율적으로 동작할 것
    
    근데 이전 동작에서 모든 클립을 가져온다는 보장이 없으므로
    원하지 않는 기능일 수 있음.
    
    Raises:
        KeyboardInterrupt: _description_
    """
    def date_range_generator():
      year = 2016 
      month = 1 
      while True:
        started_at = f"{year}-{str(month).zfill(2)}-01T00:00:00Z"
        if month == 12:
          month = 1 
          year += 1
        else:
          month += 1
        if year >= 2023 and month > 2:
          break
        # 마지막 날 5분으로 설정해서 혹시 놓치는 값 없는가 확인
        ended_at = f"{year}-{str(month).zfill(2)}-01T00:05:00Z" 
        yield (started_at, ended_at)
    
    num_of_clips = 0
    with tqdm() as progress_bar:
      for (started_at, ended_at) in date_range_generator():
        progress_bar.set_description(f"read clips in range {started_at} ~ {ended_at}")
        clips = {}
        after = ""
        tries = 0
        while tries < 3:
          try:
            clips = self.read_clips(after, started_at, ended_at)
            data = clips['data']
            pagination = clips['pagination']
            num_of_clips += len(data)
            if len(data) > 0:
              self.database.insertmany_item(self.loginName, data)
              progress_bar.update(len(data))
            if 'cursor' not in pagination:
              break
            after = pagination['cursor']
          except KeyboardInterrupt:
            raise KeyboardInterrupt
          except:
            tries += 1
        if tries >= 3:
          print(f"\n[{datetime.now()}] Failed while requesting ({after}, {started_at}, {ended_at}) => {clips}", flush=True)
  
    print(f"total clips with duplicated: {num_of_clips}")


  def download_clip(self, clip, downloadDirectory, saveJson):
    """ 
    '2017-12-29T13:12:23Z' -> '2017-12-29T13:12:23'

    주어진 포맷의 timestamp가 방송하는사람 기준인 듯?
    그러니까 저 시간이 실제 클립 만들어진 시간임.
    """
    created_at = datetime.fromisoformat(clip['created_at'][:-1]).astimezone(timezone(timedelta(hours=9)))
    year = str(created_at.year).zfill(4)
    month = str(created_at.month).zfill(2)
    day = str(created_at.day).zfill(2)
    hour = str(created_at.hour).zfill(2)
    minute = str(created_at.minute).zfill(2)
    second = str(created_at.second).zfill(2)
    
    clip_title = truncate_string_in_byte_size(clip['title'].strip())
    clip_id = clip["id"][:10]
    title = f"[{year}{month}{day}-{hour}{minute}{second}] {clip_title} ({clip_id})"
    title = replace_invalid_filename(title)
    fileDirectory = filename = os.path.join(
      downloadDirectory, 
      clip['broadcaster_name'], 
      year,
      f"{year}-{month}",
      f"{year}-{month}-{day}",
    )
    filename = os.path.join(
      fileDirectory,
      title,
    )
    clip_filename = f"{filename}.mp4"
    
    # set as pending
    clip['download_status'] = 2
    clip['download_path'] = os.path.realpath(clip_filename)
    
    # 파일 존재 확인은 건너뛰고
    # 이전의 forceDownload와 db값으로만 판별함.
    os.makedirs(fileDirectory, exist_ok=True)
    proxy_option = [] if self.proxy == None else ["--http-proxy", self.proxy]
    commands = [sys.executable, "-m", "streamlink", "-o", clip_filename, "--force"] + proxy_option + [clip["url"], "best"]
    
    tries = 1
    while tries < 4:
      completed_process = subprocess.run(
        commands,
        capture_output=True
      )
      return_code = completed_process.returncode
      if return_code == 0:
        break

      print(f"\n[{datetime.now()}][{tries}th try] Error: {clip['title']} {clip['url']}", flush=True)
      print(f"\n{completed_process.stdout}", flush=True)
      print(f"\n{completed_process.stderr}", flush=True)
      print(f"\n[{datetime.now()}][{tries}th try] retry download left {3 - tries} ", flush=True)
      time.sleep(3)
      tries += 1
    
    if tries >= 3:
      print(f"\n[{datetime.now()}] Failed to download {clip}", flush=True)
      return clip
    if tries > 1:
      print(f"\n[{datetime.now()}] Success to download {clip}", flush=True)

    if saveJson == True:
      json_filename = f"{filename}.json"
      with open(json_filename, 'w', encoding="utf-8") as json_target:
        json.dump(clip, json_target, indent=2, ensure_ascii=False)

    # set as downloaded
    clip['download_status'] = 1
    return clip

  
  
  def download_clips_from_database(self, downloadDirectory, concurrency, saveJson, forceDownload):
    def clip_handler(clip):
      if forceDownload == False and clip['download_status'] == 1:
        return clip
      return self.download_clip(clip, downloadDirectory, saveJson)
    self.database.iterate_rows(self.loginName, clip_handler, concurrency)
    
    
  
