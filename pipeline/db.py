# -*- coding: utf-8 -*-

import os.path

import sqlalchemy
from sqlalchemy.orm import sessionmaker


DATA_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                         os.path.pardir,
                         'data')
engine = sqlalchemy.create_engine('sqlite:///%s/dados.db' % DATA_PATH)
engine.raw_connection().connection.text_factory = str
Session = sessionmaker(bind=engine)
session = Session()
