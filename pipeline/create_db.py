# -*- coding: utf-8 -*-

import os.path

import db
import models


proposicoes_path = os.path.join(db.DATA_PATH, 'proposicoes.csv')
votacoes_path = os.path.join(db.DATA_PATH, 'votacoes_proposicoes.json')

session = db.session
models.Base.metadata.create_all(db.engine)
proposicoes = models.Proposicao.create_from_csv(proposicoes_path)
for proposicao in proposicoes:
    session.merge(proposicao)
votacoes = models.Votacao.create_from_file(session, votacoes_path)
session.add_all(votacoes)
session.commit()
