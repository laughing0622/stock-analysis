from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config.settings import DB_URI
from database.models import Base

class DBManager:
    def __init__(self):
        self.engine = create_engine(DB_URI, echo=False)
        self.Session = sessionmaker(bind=self.engine)

    def init_db(self):
        """创建所有表"""
        Base.metadata.create_all(self.engine)

    def get_session(self):
        return self.Session()
    
    def get_engine(self):
        return self.engine

db_manager = DBManager()