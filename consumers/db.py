import json
from sqlalchemy import create_engine, Column, Float, String, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import DatabaseError
with open("./secret.json", "r") as f:
    key = json.load(f)
Base = declarative_base()
BROKER = key.get('BROKER')


class BMeal(Base):
    __tablename__ = "breakfast"
    date = Column(DateTime, primary_key=True, unique=True, nullable=False)
    weekday = Column(String, nullable=False)
    b_diners = Column(String, nullable=False)
    event = Column(String, nullable=False)
    menu1 = Column(String, nullable=False)
    menu2 = Column(String, nullable=False)


class LMeal(Base):
    __tablename__ = "lunch"
    date = Column(DateTime, primary_key=True, unique=True, nullable=False)
    weekday = Column(String, nullable=False)
    l_diners = Column(String, nullable=False)
    event = Column(String, nullable=False)
    menu1 = Column(String, nullable=False)
    menu2 = Column(String, nullable=False)


class DMeal(Base):
    __tablename__ = "dinner"
    date = Column(DateTime, primary_key=True, unique=True, nullable=False)
    weekday = Column(String, nullable=False)
    d_diners = Column(String, nullable=False)
    event = Column(String, nullable=False)
    menu1 = Column(String, nullable=False)
    menu2 = Column(String, nullable=False)


class Weather(Base):
    __tablename__ = "weather"
    date = Column(DateTime, primary_key=True, unique=True, nullable=False)
    rainfall = Column(Float, nullable=False)
    avg_rh = Column(Float, nullable=False)
    max_temp = Column(Float, nullable=False)
    min_temp = Column(Float, nullable=False)
    avg_temp = Column(Float, nullable=False)
    di_b = Column(String, nullable=False)
    di_l = Column(String, nullable=False)
    di_d = Column(String, nullable=False)


class Menu(Base):
    __tablename__ = "menu"
    menu = Column(String, primary_key=True, unique=True)


def dbconnect() -> tuple:
    url = f'postgresql://{key.get("ID")}:{key.get("PW")}@{key.get("HOST")}:{key.get("PORT")}/{key.get("DB")}'

    engine = create_engine(url, echo=False)
    SessionLocal = sessionmaker(autocommit=False, autoflush=True, bind=engine)
    return engine, SessionLocal()


if __name__ == "__main__":
    engine, db = dbconnect()
    # create Table
    Base.metadata.create_all(bind=engine)
