from concurrent.futures import ThreadPoolExecutor, as_completed
import sqlite3
from tqdm import tqdm
from datetime import datetime

class Database:
  def __init__(self, databasePath) -> None:
    self.path = databasePath
    self.connection: sqlite3.Connection = sqlite3.connect(databasePath)

  def __del__(self):
    self.connection.close()


class ClipDatabase(Database):
  def __init__(self, databasePath) -> None:
    super().__init__(databasePath)
  
    
  def create_table(self, loginName):
    cursor = self.connection.cursor() 
    cursor.execute(f'''
CREATE TABLE IF NOT EXISTS clips_{loginName} (
  _id INTEGER PRIMARY KEY AUTOINCREMENT,
  id TEXT UNIQUE,
  url TEXT,
  embed_url TEXT,
  broadcaster_id TEXT,
  broadcaster_name TEXT,
  creater_id TEXT,
  creater_name TEXT,
  video_id TEXT,
  game_id TEXT,
  language TEXT,
  title TEXT,
  view_count INTEGER,
  created_at TEXT,
  thumbnail_url TEXT,
  duration REAL,
  vod_offset INTEGER,
  vod_url TEXT,
  download_status INTEGER DEFAULT 0,
  download_path TEXT DEFAULT "",
  updated_at TIMESTAMP
);
''')
    self.connection.commit()
    cursor.close()


  def map_row_with_schema(self, row):
    schema = [
      '_id', 'id', 'url', 'embed_url', 'broadcaster_id',
      'broadcaster_name', 'creater_id', 'creater_name', 'video_id', 'game_id',
      'language', 'title', 'view_count', 'created_at', 'thumbnail_url',
      'duration', 'vod_offset', 'vod_url', 'download_status', 'download_path',
      'updated_at',
    ]
    if len(row) != len(schema): 
      raise Exception(f"{row} and {schema} length mismatch")
    result = {}
    for i in range(len(row)):
      result[schema[i]] = row[i]
    return result 
      

  def insert_item(self, loginName: str, clip: dict):
    cursor = self.connection.cursor() 
    if clip['vod_offset'] == None:
      clip['vod_offset'] = -1
    clipValues = tuple(clip.values())
    cursor.execute(f'''
    INSERT OR IGNORE INTO clips_{loginName}(
      id, url, embed_url, broadcaster_id, broadcaster_name,
      creater_id, creater_name, video_id, game_id, language, 
      title, view_count, created_at, thumbnail_url, duration, 
      vod_offset, vod_url, updated_at
    ) VALUES (
      ?,?,?,?,?,
      ?,?,?,?,?,
      ?,?,?,?,?,
      ?,?,?
    ) ON CONFLICT (id) 
    DO UPDATE SET updated_at=?, view_count=?;''', 
    clipValues + (clip['updated_at'], clip['view_count'])
    )
    self.connection.commit()
    cursor.close()
    
  
  def insertmany_item(self, loginName: str, clips: list[dict]):
    cursor = self.connection.cursor() 
    for clip in clips:
      if clip['vod_offset'] == None:
        clip['vod_offset'] = -1
      clipValues = tuple(clip.values())
      cursor.execute(f'''
      INSERT OR IGNORE INTO clips_{loginName}(
        id, url, embed_url, broadcaster_id, broadcaster_name,
        creater_id, creater_name, video_id, game_id, language, 
        title, view_count, created_at, thumbnail_url, duration, 
        vod_offset, vod_url, updated_at
      ) VALUES (
        ?,?,?,?,?,
        ?,?,?,?,?,
        ?,?,?,?,?,
        ?,?,?
      ) ON CONFLICT (id) 
      DO UPDATE SET updated_at=?, view_count=?;''', 
      clipValues + (clip['updated_at'], clip['view_count']))
    self.connection.commit()
    cursor.close()
  
  
  def get_latest_created_at(self, loginName: str) -> str:
    cursor = self.connection.cursor()
    try:
      latest_created_at = cursor.execute(f'''
        SELECT MAX(created_at) FROM clips_{loginName}
      ''').fetchone()[0] # '2017-12-29T13:12:23Z'
      created_at = datetime.fromisoformat(latest_created_at[:-1])
      return (created_at.year, created_at.month)
    except Exception as e:
      return (2016, 1)
    finally:
      cursor.close()

  
  def update_download_info(self, loginName:str, clip: dict):
    cursor = self.connection.cursor() 
    cursor.execute(f'''
    UPDATE clips_{loginName} SET download_status=?, download_path=? WHERE _id=?
    ''', (clip['download_status'], clip['download_path'], clip['_id']))
    self.connection.commit()
    cursor.close()


  def iterate_rows(self, loginName: str, callback, concurrency: int, minView: int, maxClips: int, forceDownload: bool = False):
    cursor = self.connection.cursor()
    row_length_query = f"SELECT count(*) FROM clips_{loginName} WHERE view_count >= ?"
    if forceDownload != True:
      row_length_query += f" AND download_status != 1"
    row_length = cursor.execute(row_length_query, (minView, )).fetchone()[0]
    
    if maxClips != -1 and maxClips < row_length:
      row_length = maxClips
    
    query = f"SELECT * FROM clips_{loginName} WHERE view_count >= ?"
    if forceDownload != True:
      query += f" AND download_status != 1"
    if maxClips != -1:
      query += f" LIMIT {maxClips}"
    cursor.execute(query, (minView, ))

    with tqdm(total=row_length, unit='clip') as progress_bar:
      with ThreadPoolExecutor(max_workers=concurrency) as executor:
        try:
          futures = [executor.submit(callback, self.map_row_with_schema(row)) for row in cursor]
          for future in as_completed(futures):
            updatedClip = future.result()
            self.update_download_info(loginName, updatedClip)
            if updatedClip['download_status'] == 1:
              progress_bar.set_description_str(f"[{loginName}] SUCCESS [{updatedClip['created_at']}]")
              progress_bar.update(1)
            else: 
              progress_bar.set_description_str(f"[{loginName}] FAIL [{updatedClip['created_at']}]")
        except KeyboardInterrupt:
          print("KeyboardInterrupt! wait for currently running jobs.")
          executor.shutdown(wait=True, cancel_futures=True)
          print("KeyboardInterrupt! exit")

    cursor.close()
  

