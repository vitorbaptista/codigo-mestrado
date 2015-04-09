# -*- coding: utf-8 -*-

import csv
import copy
import sys
import argparse
from itertools import groupby

import pipeline.db as db
import pipeline.models as models


class VotesToCSV(object):
    def __init__(self):
        self.parser = self._create_parser()

    def run(self):
        options = self.parser.parse_args(sys.argv[1:])

        self.filename = str(options.legislature)
        start_date, end_date = self._legislature_dates(options.legislature)
        inside_year = models.Votacao.data.between(start_date,
                                                  end_date)
        votos = db.session.query(models.Voto)\
                  .order_by(models.Voto.parlamentar_id)\
                  .order_by(models.Voto.parlamentar_partido)\
                  .join(models.Votacao)\
                  .filter(inside_year)\
                  .all()

        votacoes = db.session.query(models.Votacao)\
                     .filter(inside_year)\
                     .order_by(models.Votacao.data)\
                     .all()
        return self._write_to_csv(votos, votacoes)

    def _write_to_csv(self, votos, votacoes):
        votacoes = [self._shallow_convert_to_dict(v) for v in votacoes]
        votacoes_ids = [v['id'] for v in votacoes]

        res = []
        votos_groupped = groupby(votos, lambda p: (p.parlamentar_id,
                                                   p.parlamentar_partido))
        for (parlamentar_id, _), votos_parlamentar in votos_groupped:
            if not parlamentar_id:
                continue
            res += [self._convert_to_vote_list(parlamentar_id,
                                               list(votos_parlamentar))]

        with open('%s.csv' % self.filename, 'w', newline='') as csv_file:
            keys = ['id', 'nome', 'party', 'uf']
            fieldnames = keys + list(votacoes_ids)
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

            writer.writeheader()
            writer.writerows(res)

        votacoes_filename = '%s-votacoes.csv' % self.filename
        with open(votacoes_filename, 'w', newline='') as csv_file:
            keys = ['id', 'id_sessao', 'proposicao_id',
                    'data', 'resumo', 'obj_votacao']
            fieldnames = keys
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

            writer.writeheader()
            writer.writerows(votacoes)

    def _legislature_dates(self, legislature):
        if legislature < 48:
            return
        start_year = 1987 + (legislature - 48) * 4
        end_year = start_year + 4

        return '%d-02-01' % start_year, '%d-01-31' % end_year

    def _convert_vote_type(self, voto):
        if voto == 'Sim':
            return 1
        elif voto == 'Não':
            return 0

    def _shallow_convert_to_dict(self, model):
        res = copy.copy(model.__dict__)
        del res['_sa_instance_state']
        return res

    def _convert_to_vote_list(self, the_id, votos):
        res = {
            'id': the_id,
            'nome': votos[0].parlamentar_nome,
            'party': votos[0].parlamentar_partido,
            'uf': votos[0].parlamentar_uf,
        }
        votos_dict = {v.votacao_id: self._convert_vote_type(v.voto)
                      for v in votos}
        res.update(votos_dict)
        return res

    def _create_parser(self):
        def _validate_legislature(legislature):
            legislature = int(legislature)
            if legislature < 48:
                msg = "Legislature must be greater or equal to 48"
                raise argparse.ArgumentTypeError(msg)
            return legislature

        parser = argparse.ArgumentParser(
            description="Converts votes in the DB to a CSV file"
        )
        parser.add_argument(
            "--legislature", type=_validate_legislature, default=54,
            help="output the votes from which legislature (default: 54)"
        )
        return parser
