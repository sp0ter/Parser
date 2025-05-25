import asyncio
import hashlib
import html
import io
import json
import logging
import os
import re
import requests
from collections import deque
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from telethon.tl.types import (
    Document, MessageMediaPhoto, MessageMediaWebPage,
    MessageEntityUrl
)

# Загрузка переменных окружения
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    filename='parser.log',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)
logger = logging.getLogger(__name__)

API_ID = int(os.getenv('TELEGRAM_API_ID'))
API_HASH = os.getenv('TELEGRAM_API_HASH')
SESSION_NAME = os.getenv('TELEGRAM_SESSION_NAME', 'anon')

client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

# Загрузка конфигурации каналов
def load_channel_config():
    try:
        with open('channel_config.json', encoding='utf-8') as f:
            return json.load(f)
    except Exception as err:
        logger.error(f"Ошибка загрузки channel_config.json: {err}")
        return {}

channel_mapping = load_channel_config()
channels = list(channel_mapping.keys())

last_processed_messages = deque(maxlen=200)
last_message_ids = {}

# Регулярные выражения для обработки ссылок и тегов
_link_re = re.compile(r'<a href="([^"]+)">(.*?)</a>')
_tag_re = re.compile(r'<[^>]+>')
url_pattern = re.compile(r'https?://\S+')

# Функция конвертации HTML в Discord Markdown
def html_to_discord_md(html_text: str) -> str:
    md = _link_re.sub(lambda m: f'[{m.group(2)}]({m.group(1)})', html_text)
    md = _tag_re.sub('', md)
    return html.unescape(md)

# Извлечение ссылок из текста сообщения
def extract_standalone_links(message):
    links = []
    if message.entities:
        for ent in message.entities:
            if isinstance(ent, MessageEntityUrl):
                start, end = ent.offset, ent.offset + ent.length
                links.append(message.text[start:end])
    for url in url_pattern.findall(message.text):
        if url not in links:
            links.append(url)
    return links

# Функция удаления упоминания WatcherGuru
def remove_watcher_guru(text):
    """Удаляет упоминание @WatcherGuru из текста."""
    return text.replace("@WatcherGuru", "").strip()

# Функция очистки текста от временных меток
def clean_message_text(text):
    """Удаляет временные метки и лишние пробелы из текста."""
    text = re.sub(r'\d{2}:\d{2}:\d{2}-\d{3}(?:\s*\+[0-1])?', '', text)
    text = re.sub(r'\s{2,}', ' ', text).strip()
    return text

# Обработка медиа-файлов
async def handle_media(message):
    if not message.media:
        return None, None
    try:
        if isinstance(message.media, (Document, MessageMediaPhoto)):
            buf = await client.download_media(message.media, file=io.BytesIO())
            buf.seek(0)
            return buf, 'media'
    except Exception as e:
        logger.error(f"Ошибка загрузки медиа: {e}")
    return None, None

# Отправка сообщения в Discord
async def send_to_discord(webhook_url, content):
    try:
        if isinstance(webhook_url, list):
            webhook_url = webhook_url[0]
        logger.info(f"Отправка в Discord: {content}")
        resp = requests.post(webhook_url, json={"content": content})
        if resp.status_code not in (200, 204):
            logger.error(f"Ошибка Discord {resp.status_code}: {resp.text}")
    except Exception as e:
        logger.error(f"Ошибка при отправке в Discord: {e}")

# Функция удаления дублирующихся ссылок
def remove_duplicate_links(text):
    """Удаляет ссылки, если они уже есть в тексте."""
    # Извлечение всех ссылок
    links = url_pattern.findall(text)
    unique_links = []
    cleaned_text = text

    for link in links:
        # Если ссылка уже упоминалась, удалить её
        if link not in unique_links:
            unique_links.append(link)
        else:
            # Удаляем повторяющиеся ссылки
            cleaned_text = cleaned_text.replace(link, "", 1).strip()

    # Удаляем лишние пробелы после удаления
    cleaned_text = re.sub(r'\s{2,}', ' ', cleaned_text).strip()
    return cleaned_text

# Основной обработчик сообщений
@client.on(events.NewMessage(chats=channels))
async def handler(event):
    try:
        chan = f'@{event.chat.username}' if event.chat.username else str(event.chat_id)
        if event.id <= last_message_ids.get(chan, 0):
            return

        formatted = ""
        try:
            html_src = getattr(event.message, 'to_html', None)
            if callable(html_src):
                formatted = html_to_discord_md(html_src())
            else:
                formatted = event.message.text or ""
            links = extract_standalone_links(event.message)
            if links:
                formatted += "\n" + "\n".join(links)
        except Exception as e:
            logger.error(f"Ошибка to_html(): {e}")
            formatted = event.message.text or ""
            links = extract_standalone_links(event.message)
            if links:
                formatted += "\n" + "\n".join(links)

        # Очистка текста от временных меток
        cleaned = clean_message_text(formatted)
        # Удаление только упоминания WatcherGuru
        cleaned = remove_watcher_guru(cleaned)
        # Удаление дублирующихся ссылок
        cleaned = remove_duplicate_links(cleaned)

        if cleaned:
            logger.info(f"Отправка в Discord: {cleaned}")
            for mapping in channel_mapping.get(chan, []):
                webhook_url = mapping[0]
                await send_to_discord(webhook_url, cleaned)

    except FloodWaitError as e:
        logger.warning(f"Telegram flood-wait: {e.seconds} секунд")
        await asyncio.sleep(e.seconds)
    except Exception as e:
        logger.error(f"Ошибка обработки сообщения: {e}")


# Функция запуска клиента
async def main():
    await client.start()
    logger.info("Бот успешно запущен")
    await client.run_until_disconnected()

if __name__ == '__main__':
    asyncio.run(main())
