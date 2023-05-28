from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData
from sqlalchemy.ext.declarative import declarative_base
def create_My_engine():
    engine = create_engine('sqlite:///YOUTUBE.db')

    meta = MetaData()

    # Base = declarative_base(bind=engine)
    # Base.metadata.drop_all(bind=engine)
    #
    # for tbl in reversed(meta.sorted_tables):
    #     tbl.drop(engine)
    #     engine.execute(tbl.delete())

    Channels = Table(
    'Channels', meta,
    Column('index', Integer),
    Column('channel_id', String, primary_key = True),
    Column('channelName', String),
    Column('Subcription', Integer),
    Column('views', Integer),
    Column('total_videos', Integer),
    Column('playlist_id', String),
    Column('description', String),
    Column('publishedAt', String),
    Column('viewCount', Integer),
    )
    meta.create_all(engine)
    return engine