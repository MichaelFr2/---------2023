import re
import asyncio
import json
import random
from pyrogram import Client
from datetime import datetime, timedelta
from pyrogram.types import Message
from pyrogram import filters
from pyrogram.handlers import MessageHandler
import logging
import logging.handlers

storage_chat = ''
storage_chat_id = int(storage_chat)

key_words_storage = ''
key_words_storage_id = int(key_words_storage)

api_id =  
api_hash = "" 
 
app = Client("my_account", api_id, api_hash, proxy=proxy)

parse_lock = asyncio.Lock()

custom_limit = 300

key_words = []

NEGATIVE_RESPONSE = "\U0001F44E"
POSITIVE_RESPONSE = "\U0001F44D"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class JsonFileHandler(logging.FileHandler):
    def __init__(self, filename, mode='a', encoding=None, delay=False):
        super().__init__(filename, mode, encoding, delay)
        with open(self.baseFilename, "r", encoding="utf-8") as file:
            self.first_record = len(file.read()) == 0

    def emit(self, record):
        log_entry = self.format(record)
        if self.first_record:
            with open(self.baseFilename, "a", encoding="utf-8") as file:
                file.write("[")
                self.first_record = False
        else:
            with open(self.baseFilename, "rb+") as file:
                file.seek(-1, 2)
                file.truncate()
                file.write(b',')
        with open(self.baseFilename, "a", encoding="utf-8") as file:
            file.write(f"{log_entry}]")

    def format(self, record):
        log_data = {
            "timestamp": datetime.fromtimestamp(record.created).strftime("%Y-%m-%d %H:%M:%S"),
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage(),
        }
        return json.dumps(log_data, ensure_ascii=False)

json_handler = JsonFileHandler("parser_logs.json")
logger.addHandler(json_handler)

last_run_time = datetime.now()

def is_after_last_run_time(message_time):
    global last_run_time
    adjusted_last_run_time = last_run_time + timedelta(hours=3)
    return message_time >= adjusted_last_run_time

async def process_message(message):
    try:
        chat_link = ""
        sender_link = ""
        sender_id = "not_found"
        message_text = ""
        message_time = ""
        reply_text = ""
        reply_user = ""
        reply_user_id = ""
        chat_title = ""
        
        if (message.chat.username is None):
            if (message.chat.id is not None ):
                chat_link = f"t.me/{message.chat.id}"
        if (message.chat.title is not None):
            chat_title = message.chat.title
        if (message.chat.username is not None) and (message.chat.id is not None):
            chat_link = f"t.me/{message.chat.username}/{message.chat.id}"
        if message.from_user is None:
            if (message.sender_chat is not None) and (message.sender_chat.username is not None):
                sender_link = f"t.me/{message.sender_chat.username}"
                sender_id = message.sender_chat.username
        else:
            sender_link = f"t.me/{message.from_user.username}"
            sender_id = message.from_user.username
        if message.text is not None:
            message_text = message.text
        if message.date is not None:
            message_time = message.date.strftime("%Y-%m-%d %H:%M:%S")
        if message.reply_to_message_id is not None:
            reply_message = await app.get_messages(message.chat.id, message.reply_to_message_id )
            if reply_message.from_user is None:
                if (reply_message.sender_chat is not None) and (reply_message.sender_chat.username is not None):
                    reply_user = f"t.me/{reply_message.sender_chat.username}"
                    reply_user_id = reply_message.sender_chat.username
            else:
                reply_user = f"t.me/{reply_message.from_user.username}"
                reply_user_id = reply_message.from_user.username
            if reply_message.text is not None:
                reply_text = reply_message.text
        result_data = [chat_link, sender_link, message_text, message_time, reply_text, reply_user, sender_id, reply_user_id, chat_title]
        result_text = (f"Chat:{result_data[0]};\n"
                        f"Chat_title:{result_data[8]};\n"
                        f"Sender:{result_data[1]};\n"
                        f"Message:{result_data[2]};\n"
                        f"Reply_to_user:{result_data[5]};\n"
                        f"Reply_to_text:{result_data[4]};\n"
                        f"Reply_sender_id:{result_data[7]};\n"
                        f"Date_time:{result_data[3]};\n"
                        f"Sender_username:{result_data[6]};\n"
                        )
                        
        await app.send_message(storage_chat_id, result_text)
    except Exception as e:
        logger.error(f"Ошибка при обработке сообщения: {e}")

async def search_messages(chat_id):
    print(f"Searching in chat: {chat_id}")
    try:
        async for message in app.search_messages(chat_id, limit=custom_limit):
            if not isinstance(message, Message):
                continue
            if not is_after_last_run_time(message.date):
                return
            if not message.text:
                continue
            if not any(keyword.lower() in message.text.lower() for keyword in key_words):
                continue
            logger.info(f"Found message: {message.text} in Chat: {chat_id}")
            await process_message(message)
    except Exception as e:
        logger.error(f"Ошибка при парсинге: {e}")  

async def chats_parsing():
    logger.info("Parsing started")
    try:
        async for chat_id in app.get_dialogs():
            if (int(chat_id.chat.id) < 0) and (int(chat_id.chat.id) != int(-)) and (int(chat_id.chat.id) != int(-)) and (int(chat_id.chat.id) != int(-)): 
                await search_messages(chat_id.chat.id)
        await app.send_message(storage_chat_id, "Парсинг завершён")
    except Exception as e:
        logger.error(f"Ошибка при итерации по чатам: {e}")
    logger.info("Parsing stopped")

def check_reaction(message):
    if message.reactions is not None and len(message.reactions.reactions) > 0:
        reaction = message.reactions.reactions[0].emoji
        if reaction == POSITIVE_RESPONSE:
            return True
    return False
    
async def get_key_words_from_storage():
    try:
        key_words.clear()
        async for message in app.get_chat_history(key_words_storage_id):
            if message.text and check_reaction(message):
                key_words.append(message.text)
        print(f'Ключевые слова: {key_words}')
    except Exception as e:
        logger.error(f"Ошибка при поиске ключевого слова: {e}")

@app.on_message(filters.command("parse") & filters.chat(storage_chat_id))
async def run_command_handler(_, message: Message):
    try:
        if not parse_lock.locked():
            await message.reply("$%Парсинг запущен")
            await get_key_words_from_storage()
            async with parse_lock:
                await chats_parsing()
        else:
            await message.reply("$%Парсинг уже запущен")
    except Exception as e:
        logger.error(f"Ошибка при обработке команды бота: {e}")

async def main():
    logger.info("App started")
    try:
        await app.start()        
        me = await app.get_me()
        logger.info(f'Script is running as {me.username}')
        while True:
            await asyncio.sleep(3 * 60 *60)  # Запуск каждые 3 часа
            await get_key_words_from_storage()
            if not parse_lock.locked():
                async with parse_lock: 
                    await chats_parsing()

        await asyncio.get_event_loop().create_future()
    except Exception as e:
        logger.error(f"Ошибка при выполнении программы: {e}")
    
    logger.info("App stopped")

if __name__ == "__main__":
    try:
        asyncio.get_event_loop().run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Program interrupted by user.")
        app.stop()
    except Exception as e:
        logger.error(f"Ошибка при выполнении программы: {e}")
        app.stop()