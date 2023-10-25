'''
Этот скрипт предназначен для резервного копирования сообщений электронной почты старше 60 дней с сервера IMAP в локальные и сетевые папки.

Этот скрипт удаляет обработанные сообщения с сервера IMAP после создания их резервных копий. Убедитесь, что это действительно то, что вы хотите делать, прежде чем запускать скрипт.
Закомментируйте эти строки, чтобы письма не удалялись
server.delete_messages(message_id)
server.expunge()

https://github.com/zevs-ik/
'''

import csv
from imapclient import IMAPClient
import email
from email.header import decode_header
import os
import shutil
from datetime import datetime, timedelta
import re

# Переменные
BACKUP_FOLDER = 'C:\\mailbackup\\eml'
NETWORK_BACKUP_FOLDER = 'Z:\\Mailbackup\\eml'
IMAP_SERVER = 'mail.example.com'
CSV_FILE_PATH = 'c:\\script\\email.csv'
DAYS_AGO = 60

def safe_filename(filename):
    # Замена недопустимых символов на подчеркивание
    filename = re.sub(r'[\\/*?:"<>|]', "_", filename)
    filename = re.sub(r'[^\x20-\x7E\u0400-\u04FF]', '', filename)
    filename = re.sub(r'\r\n', "", filename)
    # Ограничение длины имени файла до 200 символов
    return filename[:200]

def backup_email(email_address, user, password):
    # Формирование путей для локального и сетевого путевых папок резервных копий
    backup_folder = os.path.join(BACKUP_FOLDER, email_address)
    network_backup_folder = os.path.join(NETWORK_BACKUP_FOLDER, email_address)
    
    try:
        with IMAPClient(IMAP_SERVER, ssl=True) as server:
            server.login(user, password)
            # Вычисление даты на основе DAYS_AGO
            date_days_ago = (datetime.now() - timedelta(days=DAYS_AGO)).strftime('%d-%b-%Y')

            for folder_info in server.list_folders():
                # Получение имени текущей папки на сервере
                folder_name = folder_info[2]
                # Выбор текущей папки на сервере
                server.select_folder(folder_name)

                # Формирование путей для локальной и сетевой папки 
                current_backup_folder = os.path.join(backup_folder, folder_name.replace('\\', '/'))
                current_network_backup_folder = os.path.join(network_backup_folder, folder_name.replace('\\', '/'))

                # Создание директорий для резервных копий, если они не существуют
                os.makedirs(current_backup_folder, exist_ok=True)
                if os.path.exists(NETWORK_BACKUP_FOLDER):
                    os.makedirs(current_network_backup_folder, exist_ok=True)

                # Поиск сообщений, которые были получены ранее указанной даты
                messages = server.search(f'BEFORE {date_days_ago}')

                for message_id in messages:
                    # Загрузка данных сообщения
                    fetch_data = server.fetch(message_id, ['RFC822'])
                    if message_id in fetch_data:
                        # Обработка данных сообщения
                        msg_data = fetch_data[message_id][b'RFC822']
                        msg = email.message_from_bytes(msg_data)
                        subject = msg.get("Subject", "(no subject)")
                        subject, encoding = decode_header(subject)[0]
                        if isinstance(subject, bytes):
                            subject = subject.decode(encoding if encoding else 'utf-8')
                        subject = safe_filename(subject)
                        # Сохранение сообщения в файл
                        file_path = os.path.join(current_backup_folder, f'{message_id}_{subject}.eml')
                        with open(file_path, 'wb') as f:
                            f.write(msg_data)
                        # Копирование файла в сетевую папку
                        if os.path.exists(NETWORK_BACKUP_FOLDER):
                            network_file_path = os.path.join(current_network_backup_folder, f'{message_id}_{subject}.eml')
                            shutil.copy(file_path, network_file_path)
                        # Удаление сообщения с сервера
                        server.delete_messages(message_id)
                        server.expunge()
                    else:
                        print(f'No data returned for message ID {message_id}')
    except ConnectionResetError as e:
        print(f'Connection reset error: {e}. Skipping {email_address}.')
        return
    except Exception as e:
        print(f'An error occurred: {e}. Skipping {email_address}.')
        return

# Чтение данных из CSV файла и запуск процесса резервного копирования
with open(CSV_FILE_PATH, newline='', encoding='utf-8') as csvfile:
    email_reader = csv.reader(csvfile, delimiter=';')
    next(email_reader, None)  # Пропуск заголовков
    for row in email_reader:
        email_address, user, password = row
        backup_email(email_address, user, password)
