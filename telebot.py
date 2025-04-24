from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
import time
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()
# 본인의 API 정보
api_id = os.getenv("API_ID")
api_hash = os.getenv("API_HASH")
channel_username = os.getenv("CHANNEL_USERNAME")

# 수집할 최대 메시지 개수 설정
total_count_limit = 1000

client = TelegramClient("my_session", api_id, api_hash)

with client:
    # 채널 객체 가져오기
    channel = client.get_entity(channel_username)

    all_messages = []
    offset_id = 0
    batch_size = 100  # 한번에 가져올 메시지 수

    while len(all_messages) < total_count_limit:
        remaining = total_count_limit - len(all_messages)
        fetch_limit = min(batch_size, remaining)

        history = client(
            GetHistoryRequest(
                peer=channel,
                offset_id=offset_id,
                offset_date=None,
                add_offset=0,
                limit=fetch_limit,
                max_id=0,
                min_id=0,
                hash=0,
            )
        )

        if not history.messages:
            break

        for message in history.messages:
            if message.message:
                all_messages.append(
                    {"id": message.id, "date": message.date, "text": message.message}
                )

        print(f"가져온 메시지 수: {len(all_messages)}")
        offset_id = history.messages[-1].id
        time.sleep(0.5)

    # CSV 저장
    df = pd.DataFrame(all_messages)
    df.to_csv("telegram_channel_messages.csv", index=False, encoding="utf-8-sig")
