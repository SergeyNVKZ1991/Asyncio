import asyncio
import datetime
import math
import sys

import aiohttp

from models import Base, Session, SwapiPeople, engine

MAX_CHUNK_SIZE = 10


async def get_field_values(session, urls, field_name):
    field_values = []
    if urls is not None:
        for url in urls:
            response = await session.get(url)
            json_data = await response.json()
            field_values.append(json_data[field_name])
    return field_values


async def get_people(people_id):
    async with aiohttp.ClientSession() as session:
        response = await session.get(f'https://swapi.dev/api/people/{people_id}')
        json_data = await response.json()

        fields = {
            'films': 'title',
            'species': 'name',
            'starships': 'name',
            'vehicles': 'name'
        }

        data = {
            'id': people_id,
            'name': json_data.get('name'),
            'gender': json_data.get('gender'),
            'hair_color': json_data.get('hair_color'),
            'height': float(json_data['height'].replace(',', '.')) if 'height' in json_data and json_data['height'].isdigit() else None,
            'homeworld': json_data.get('homeworld'),
            'mass': float(json_data['mass'].replace(',', '.')) if 'mass' in json_data and json_data['mass'] != 'unknown' else None,
            'skin_color': json_data.get('skin_color'),
            'birth_year': json_data.get('birth_year'),
            'eye_color': json_data.get('eye_color')
        }

        for field, field_name in fields.items():
            field_urls = json_data.get(field)
            field_values = await get_field_values(session, field_urls, field_name)
            data[field] = ', '.join(field_values) if field_values else 'None'

        await session.close()

    return data


async def get_total_people_count():
    session = aiohttp.ClientSession()
    response = await session.get('https://swapi.dev/api/people/')
    json_data = await response.json()
    await session.close()
    count = json_data['count']
    return count


async def insert_to_db(people_list):
    async with Session() as session:
        swapi_people_list = [
            SwapiPeople(**person_data) for person_data in people_list
        ]
        session.add_all(swapi_people_list)
        await session.commit()


async def main():
    async with engine.begin() as con:
        await con.run_sync(Base.metadata.create_all)

    total_people = await get_total_people_count()
    num_chunks = math.ceil(total_people / MAX_CHUNK_SIZE)

    tasks = []
    for chunk_index in range(num_chunks):
        start_id = chunk_index * MAX_CHUNK_SIZE + 1
        end_id = min((chunk_index + 1) * MAX_CHUNK_SIZE, total_people)
        ids_chunk = range(start_id, end_id + 1)

        get_people_coros = [get_people(people_id) for people_id in ids_chunk]
        people_json_list = await asyncio.gather(*get_people_coros)
        tasks.append(await insert_to_db(people_json_list))

    current_task = asyncio.current_task()
    tasks_sets = asyncio.all_tasks()
    tasks_sets.remove(current_task)

    await asyncio.gather(*tasks_sets)
    await engine.dispose()


if __name__ == '__main__':
    start = datetime.datetime.now()
    if sys.version_info[0] == 3 and sys.version_info[1] >= 8 and sys.platform.startswith('win'):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
    print(datetime.datetime.now() - start)
