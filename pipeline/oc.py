# -*- coding: utf-8 -*-
import csv
import copy
from itertools import groupby

import db
import models


filename = 'oc-54'
# 52a legislatura: 2003-02-01 -> 2007-01-31
# 53a legislatura: 2007-02-01 -> 2011-01-31
# 54a legislatura: 2011-02-01 -> 2015-01-31
inside_year = models.Votacao.data.between('2011-02-01',
                                          '2015-01-31')
votos = db.session.query(models.Voto)\
          .order_by(models.Voto.parlamentar_id)\
          .order_by(models.Voto.parlamentar_partido)\
          .join(models.Votacao)\
          .filter(inside_year)\
          .all()

# Preciso da lista de parlamentares
# Lista de votacoes
# Lista de votos


def _convert_vote_type(voto):
    if voto == 'Sim':
        return 1
    elif voto == 'Não':
        return 0


def _shallow_convert_to_dict(model):
    res = copy.copy(model.__dict__)
    del res['_sa_instance_state']
    return res

votacoes = db.session.query(models.Votacao)\
             .filter(inside_year)\
             .order_by(models.Votacao.data)\
             .all()

votacoes = [_shallow_convert_to_dict(v) for v in votacoes]
votacoes_ids = [v['id'] for v in votacoes]


def _convert_to_vote_list(the_id, votos):
    res = {
        'id': the_id,
        'nome': votos[0].parlamentar_nome,
        'party': votos[0].parlamentar_partido,
        'uf': votos[0].parlamentar_uf,
    }
    votos_dict = {v.votacao_id: _convert_vote_type(v.voto)
                  for v in votos}
    res.update(votos_dict)
    return res


# parlamentar, votacao_1, votacao_2, votacao_3
# 123423, 1, 0, 1
# 312432, 0, 0, 0
res = []
votos_groupped = groupby(votos, lambda p: (p.parlamentar_id,
                                           p.parlamentar_partido))
for (parlamentar_id, parlamentar_partido), votos_parlamentar in votos_groupped:
    if not parlamentar_id:
        continue
    res += [_convert_to_vote_list(parlamentar_id,
                                  list(votos_parlamentar))]

with open('%s.csv' % filename, 'w', newline='') as csv_file:
    keys = ['id', 'nome', 'party', 'uf']
    fieldnames = keys + list(votacoes_ids)
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

    writer.writeheader()
    writer.writerows(res)

with open('%s-votacoes.csv' % filename, 'w', newline='') as csv_file:
    keys = ['id', 'id_sessao', 'proposicao_id',
            'data', 'resumo', 'obj_votacao']
    fieldnames = keys
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

    writer.writeheader()
    writer.writerows(votacoes)
