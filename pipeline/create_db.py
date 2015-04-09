# -*- coding: utf-8 -*-

import sqlalchemy
import os.path

import db
import models


def _create_and_populate_db():
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


def _normalize_parties_names():
    """Normalize parties that changed name in a single one

    The source data comes from the CEBRAP database's table tbl_Partido.
    """
    def _generate_normalizations(model_party, model_party_attr_name):
        def renomeia_partido(nome_antigo, nome_novo):
            nome = '%s>%s' % (nome_antigo, nome_novo)
            return {
                'filter': model_party.in_([nome_antigo, nome_novo]),
                'update': {model_party_attr_name: nome},
            }

        normalizations = {
            'PCB>PPS': renomeia_partido('PCB', 'PPS'),
            'PDS>PP': renomeia_partido('PDS', 'PP'),
            'PFL>DEM': renomeia_partido('PFL', 'DEM'),
            'PJ>PTC': renomeia_partido('PJ', 'PTC'),
            'PMR>PRB': renomeia_partido('PMR', 'PRB'),
            'PL+PRONA>PR': {
                'filter': model_party.in_(['PL', 'PRONA', 'PR']),
                'update': {model_party_attr_name: 'PL+PRONA>PR'},
            },
            'SD': {
                'filter': model_party.in_(['SDD', 'Solidaried']),
                'update': {model_party_attr_name: 'SD'},
            },
            'PCDOB': {
                'filter': (sqlalchemy.func.lower(model_party) == 'pcdob'),
                'update': {model_party_attr_name: 'PCdoB'},
            }
        }

        return normalizations

    normalizations = _generate_normalizations(models.Voto.parlamentar_partido,
                                              'parlamentar_partido')
    for normalization in normalizations.values():
        db.session.query(models.Voto)\
          .filter(normalization['filter'])\
          .update(normalization['update'], synchronize_session=False)

    normalizations = _generate_normalizations(models.Proposicao.autor_partido,
                                              'autor_partido')
    for normalization in normalizations.values():
        db.session.query(models.Proposicao)\
          .filter(normalization['filter'])\
          .update(normalization['update'], synchronize_session=False)

    db.session.commit()

if __name__ == '__main__':
    _create_and_populate_db()
    _normalize_parties_names()
