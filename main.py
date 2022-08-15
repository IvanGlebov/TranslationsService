import time
import json
import yaml
import os
import pandas as pd
# import google.auth
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import gspread
from git import Repo

import authorize

config = {}

en_alphabet = 'abcdefghijklmnopqrstuvwxyz'

def main():

  # print('config')
  # print(config)
  creds = authorize.authorize()
  

  try:
    
    # Подключаемся к Google Drive сервису
    driveService = build('drive', 'v3', credentials=creds)
    
    # Подключаемся к Google Spreadsheets сервису
    # sheetsService = build('sheets', 'v4', credentials=creds)

    # Подключаемся к Google Spreadsheets сервису через библиотеку gsheet
    gc = gspread.authorize(creds)
    
    # Список для хранения всех файлов/папок
    files = []
    # Список для хранения всех листов тааблиц
    sheets = []
    # Токен новой страницы. Если он не None, то есть ещё одна страница и надо запрашивать дальше
    page_token = None
    # Список для хранения спаршенных данных из таблиц в yaml
    yaml_files = []

    # Цикл для получения всех файлов/папок пока page_token не будет равен None
    while True:
      # Запрашиваем все директории внутри базовой папки. Данные в формате {id, name}
      response = driveService.files().list(q=f"mimeType='application/vnd.google-apps.folder' and '{config.get('base_folder_id')}' in parents",
                                      spaces='drive',
                                      fields='nextPageToken, '
                                              'files(id, name)',
                                      pageToken=page_token).execute()
      
      files.extend(response.get('files', []))
      page_token = response.get('nextPageToken', None)
      if page_token is None:
        break

    # Пробегаемся по всем полученным папкам
    for f in files:
      # Получаем все файлы внутри текущей папки с типом spreadsheet и с именем config.get('big_table_name')
      innerFiles = driveService.files().list(q=f"mimeType='application/vnd.google-apps.spreadsheet' and name='{config.get('big_table_name')}' and '{f.get('id')}' in parents",
                                      spaces='drive',
                                      fields='nextPageToken, '
                                              'files(id, name)',
                                      pageToken=page_token).execute()
      # print(F"Files found in {file.get('name')}:")

      # Получаем список страниц используя генератор. В генераторе мы пробегаемся по файлам с полями {id, name}. Каждый элемент мы открываем по его id
      # у каждого файла мы выбираем первую таблицу и забираем все значения оттуда. Полученную структуру данных загоняем в pandas и парсим в DataFrame
      # используя pd.DataFrame.from_records()
      sheets.extend([pd.DataFrame.from_records(gc.open_by_key(sheet.get('id')).sheet1.get_all_values()) for sheet in innerFiles.get('files', [])])

      # Получение страниц таблиц, но без использования генератора
      # for sheet in innerFiles.get('files', []):
      #   table = gc.open_by_key(sheet.get('id')).sheet1
      #   df = pd.DataFrame.from_records(table.get_all_values())
      #   sheets.append(df)

    # print(sheets)
    # print(sheets[0])
    # df = sheets[0]
    
    for index, df in enumerate(sheets):
      print(F"Collecting translations for file {index}")
      new_header = df.iloc[0]
      df = df[1:]
      df.columns = new_header
      df_headers = [header.lower() for header in df.columns.values.tolist()]
      new_df_headers = []
      for col in df_headers:
        temp_df = df[col]
        temp_df = temp_df.dropna()
        if temp_df.size >= df['en'].size / 4:
          new_df_headers.append(temp_df.name)
      data = df[[new_df_headers[0]]].dropna()
      new_df = pd.merge(data, df, on=new_df_headers[0])
      new_df = new_df[new_df.columns.intersection(new_df_headers)]
      new_df['alias'] = new_df[[new_df_headers[config.get('en_header_number')]]]
      res = new_df.to_dict('records')
      res = yaml.dump(res, default_flow_style=False)
      yaml_files.append(res)
    print('Tables colecction finished')


    # Клонирование и обновление репозитория
    # Создаём временную папку для репозитория
    if not os.path.exists(os.path.join(os.getcwd(), "TempDir")):
      print('Создаём папку под репозиторий')
      os.mkdir(os.path.join(os.getcwd(), "TempDir"))
    # Сохраняем полный поть до нашего репозитория
    repo_path = os.path.join(os.getcwd(), 'TempDir')

    repo = ''
    if len(os.listdir(repo_path)) == 0:
      # Клонируем репозиторий
      print('Клонируем репозиторий')
      repo = Repo.clone_from(config.get('repo_url'), repo_path)
      print(os.listdir())
      # print(yaml_files)
    else:
      print('Репозиторий уже представлен. Обновляем...')
      repo = Repo('TempDir')
      for remote in repo.remotes:
        remote.fetch()
    
    # Получение файла со старыми переводами
    old_translations = ''
    with open(os.path.join(repo_path, 'src/data/locales/translations.yaml'), 'r') as stream:
      try:
        old_translations = yaml.safe_load(stream)
      except yaml.YAMLError as exc:
        print(exc)
    # print(old_translations)

    languagesCounter = {}
    for new_translation_file in yaml_files:
      new_translations = ''
      try:
        new_translations = yaml.safe_load(new_translation_file)
      except yaml.YAMLError as exc:
        print(exc)  
      for row in new_translations:
        # presentFlag = False
        for index, r in enumerate(old_translations):
          if r['alias'] == row['alias']:
            # presentFlag = True
            temp_old_translations = dict(r)
            temp_new_translations = dict(row)
            del temp_new_translations['alias']
            # if __mode == 'overwrite':
            for idx, lang in enumerate(temp_new_translations):
              if temp_new_translations[lang] != '':
              # If translation is not equal
                try:
                  if temp_old_translations[lang] != temp_new_translations[lang]:
                    languagesCounter[lang] = languagesCounter[lang] + 1
                    temp_old_translations[lang] = temp_new_translations[lang]
                except KeyError as key:
                  languagesCounter[lang] = 1
                  temp_old_translations[lang] = temp_new_translations[lang]
            old_translations[index] = temp_old_translations
    # Выгрузка обновлённого файла
    with open(os.path.join(repo_path, 'src/data/locales/translations.yaml'), 'w') as stream:
      try:
        yaml.dump(old_translations, stream, default_flow_style=False)
      except yaml.YAMLError as exc:
        print(exc)

    # Считаем обновлённые языки. Язык указывается если количество обновлённых строк больше половины от максимального
    maxValue = 0
    for el in languagesCounter:
      if languagesCounter[el] > maxValue:
        maxValue = languagesCounter[el]

    final_languages = [el for el in languagesCounter if languagesCounter[el] >= maxValue / 2 and languagesCounter[el] > 2]
    
    languages = ''
    for lang in final_languages:
      if languages != '':
        languages += f", {lang}"
      else:
        languages += lang

    # Делаем коммит и пушим
    try:
      repo.git.add(update=True)
      repo.index.commit(f'Automatic translations update for: {languages}')
      origin = repo.remote(name='origin')
      origin.push()
    except error:
      print(f"Error pushing: {error}")  
  except HttpError as error:
    print(f'An error occurred: {error}')



if __name__ == '__main__':
  # Бесконечная работа по интервалу
  while True:
    # Читаем файл конфига
    with open('config.json', 'r') as stream:
      config = json.load(stream)

    main()
    print(f"Sleep for {config.get('days')} days, {config.get('hours')} hours, {config.get('minutes')} minutes and {config.get('seconds')} seconds")

    # Ожидаем следующего запуска
    time.sleep(config.get('seconds') + config.get('minutes') * 60 + config.get('hours') * 3600 + config.get('days') * 86400)
