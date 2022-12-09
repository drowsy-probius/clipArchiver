import argparse
import sqlite3
from datetime import datetime



def migrate(database_path):
  def table_names(cursor: sqlite3.Cursor):
    cursor.execute('''
      SELECT 
          name
      FROM 
          sqlite_schema
      WHERE 
          type ='table' AND 
          name NOT LIKE 'sqlite_%';
    ''')
    tables = cursor.fetchall()
    tables = [i[0] for i in tables]
    return tables 
  
  connection = sqlite3.connect(database_path)
  cursor = connection.cursor()
  cursor2 = connection.cursor()
  
  tables = table_names(cursor)
  for table in tables:

    # update table 
    try:
      cursor.execute(f'ALTER TABLE {table} ADD COLUMN vod_url TEXT;')
    except Exception as e:
      print(f'vod_url creation error: {e}')
    try:
      cursor.execute(f'ALTER TABLE {table} ADD COLUMN updated_at TIMESTAMP;')
    except Exception as e:
      print(f'updated_at creation error: {e}')

    
    try:
      # update vod_url, updated_at column 
      cursor.execute(f'SELECT _id, thumbnail_url FROM {table}')
      for clip in cursor:
        _id, thumbnail_url = clip 
        vod_url = thumbnail_url[:thumbnail_url.index('-preview-')] + '.mp4'
        updated_at = datetime.now() 
        cursor2.execute(f'''
        UPDATE {table} SET vod_url=?, updated_at=? WHERE _id=?;
        ''', (vod_url, updated_at, _id))
        
    except Exception as e:
      print(f'UPDATE CLIPS ERROR: {e}')
  
  connection.commit()
  cursor.close()
  cursor2.close()


if __name__ == "__main__":
  parser = argparse.ArgumentParser(
    prog="Twitch Clip Archiver DB Migrator",
    description="Add vod_url, updated_at columns to given database",
  )
  
  parser.add_argument('databases', type=str, nargs='+',
                      help='databases to migrate')
  
  databases = parser.parse_args().databases 
  
  for database in databases:
    migrate(database)
    print(f'{database} DONE!')
  
  