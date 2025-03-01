import yaml
import pandas as pd 
from sqlalchemy import create_engine
from db import engine 
from sqlalchemy import create_engine, Column, String, Float, Integer, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Buyer(Base):
    __tablename__ = 'buyers'
    id = Column(Integer, primary_key=True, autoincrement=True)
    buyer_id = Column(String(50))
    preferred_grade = Column(String(50))
    preferred_finish = Column(String(50))
    preferred_thickness_mm = Column(Float)
    preferred_width_mm = Column(Float)
    max_weight_kg = Column(Float)
    min_quantity = Column(Integer)

class Supplier(Base):
    __tablename__ = 'suppliers'
    id = Column(Integer, primary_key=True, autoincrement=True)
    supplier_id = Column(String(50))
    article_id = Column(String(50))
    material = Column(String(100))
    grade = Column(String(50))
    quality_choice = Column(String(50))
    finish = Column(String(50))
    thickness_mm = Column(Float)
    width_mm = Column(Float)
    weight_kg = Column(Float)
    quantity = Column(Integer)
    description = Column(String(255))
    reserved_status = Column(String(50))


def create_tables():
    """Create tables in the database"""
    Base.metadata.create_all(engine)
    print(f"Successfully created tables in {engine.url.database}")

if __name__ == '__main__':
    create_tables()