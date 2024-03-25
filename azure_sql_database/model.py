#staging.model.py
from sqlalchemy import inspect, create_engine, Column, Float, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from contextlib import contextmanager

Base = declarative_base()

class Database():
    def __init__(self, database_url):
        self.engine = create_engine(database_url, echo=False)
        self.Session = scoped_session(sessionmaker(bind=self.engine))

    @contextmanager
    def session(self):
        """Provide a transactional scope around a series of operations, intended for read-only use."""
        session = self.Session()
        try:
            yield session
            # No commit or rollback is needed for read-only operations
        finally:
            session.close()


class Correlation(Base):
    __tablename__ = "Correlation"
    weatherFactor = Column(String)
    pollutionFactor = Column(String)
    pearson = Column(Float, primary_key=True, autoincrement=False)

    def __repr__(self):
        return f'Correlation: {self.weatherFactor}, {self.pollutionFactor}, {self.pearson}'
