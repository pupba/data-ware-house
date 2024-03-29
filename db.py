from hashlib import sha256
import json
from sqlalchemy import create_engine, Column, Float, String, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base

Base = declarative_base()


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


class User(Base):
    __tablename__ = "users"
    id = Column(String, primary_key=True, nullable=True)
    pw = Column(String, nullable=True, unique=True)


def encryption(password: str) -> str:
    return sha256(password.encode()).hexdigest()


def dbconnect() -> tuple:
    with open("./secret.json", "r") as f:
        key = json.load(f)
    url = f'postgresql://{key.get("ID")}:{key.get("PW")}@{key.get("HOST")}:{key.get("PORT")}/{key.get("DB")}'
    # url = f'postgresql://{key.get("ID")}:{key.get("PW")}@{key.get("HOST")}:{key.get("PORT")}/users'

    engine = create_engine(url, echo=False)
    SessionLocal = sessionmaker(autocommit=False, autoflush=True, bind=engine)
    return engine, SessionLocal()


if __name__ == "__main__":
    engine, db = dbconnect()
    # Weather.metadata.create_all(bind=engine)
    # BMeal.metadata.create_all(bind=engine)
    # LMeal.metadata.create_all(bind=engine)
    # DMeal.metadata.create_all(bind=engine)
    # User.metadata.create_all(bind=engine)
    # data = User(id="admin", pw=encryption("qwer1234"))
    # db.add(data)
    # db.commit()
    # db.close()
    # create Table
    # Base.metadata.create_all(bind=engine)
