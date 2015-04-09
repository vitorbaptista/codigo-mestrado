# -*- coding: utf-8 -*-

import os
import csv
import collections
from datetime import datetime

import pipeline.db as db
import pipeline.models as models


class PartiesAndCoalitionsChanges(object):
    def run(self):
        path = 'parties_and_coalitions_changes.csv'
        result = self._legislators_groupped_by_party_and_coalition()

        with open(path, 'w', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=result[0].keys())
            writer.writeheader()
            writer.writerows(result)

    def _legislators_groupped_by_party_and_coalition(self):
        """Returns a list of legislators with one entry per coalition change

        For example, if a legislators' party got in and out of the coalition,
        him will be returned twice (once when he was in the coalition, and once
        when he was out). If he switched parties but they both were in (or out)
        the coalition, he would be returned only once (the first one).
        """
        coalizoes = self._get_coalizoes()
        coalizoes_partidos = self._get_coalizoes_partidos()
        results = []
        for coalizao in coalizoes:
            partidos_na_coalizao = coalizoes_partidos[coalizao["Id_Clz"]]

            start_date = coalizao["DataInicial"]
            end_date = coalizao["DataFinal"]
            votos_coalizao = self._get_parlamentares_between(start_date,
                                                             end_date)
            results = results + self._add_coalizao_column(votos_coalizao,
                                                          partidos_na_coalizao)

        # uniqify
        key = lambda v: v["name"] + str(v["coalizao"])
        # I'm using reversed to keep the first date, as it's sorted by date asc
        results = list({key(r): r for r in reversed(results)}.values())

        results = self._remove_unique_rows(results, "id")
        sort_keys = lambda v: (v["id"] or -1, v["rollcall_date"])
        return sorted(results, key=sort_keys)

    def _remove_unique_rows(self, rows, key):
        keys = [row[key] for row in rows]
        unique_values = [val for val, count
                         in collections.Counter(keys).items()
                         if count == 1]

        return [row for row in rows if row[key] not in unique_values]

    def _add_coalizao_column(self, votos_coalizao, partidos_coalizao):
        result = [collections.OrderedDict(v) for v in votos_coalizao]
        for voto in result:
            voto["coalizao"] = voto["party"] in partidos_coalizao
        return result

    def _get_parlamentares_between(self, start_date, end_date):
        between = models.Votacao.data.between(start_date,
                                              end_date)
        votos = db.session.query(models.Voto)\
                  .join(models.Votacao)\
                  .filter(between)\
                  .group_by(models.Voto.parlamentar_id)\
                  .group_by(models.Voto.parlamentar_partido)\
                  .order_by(models.Voto.parlamentar_id)\
                  .order_by(models.Votacao.data)\
                  .all()
        votos = [self._convert_to_dict(v) for v in votos]
        return votos

    def _get_coalizoes(self):
        path = os.path.join(db.DATA_PATH, 'tbl_CoalizaoP.csv')
        with open(path, 'r') as csv_file:
            reader = csv.DictReader(csv_file)
            return [self._parse_coalizao(row) for row in reader]

    def _parse_coalizao(self, coalizao):
        parse_date = lambda d: datetime.strptime(d, "%Y-%m-%d %H:%M:%S")
        date_keys = ["DataInicial", "DataArgelina",
                     "DataMediana", "DataFinal",
                     "DataLeg"]
        for key in date_keys:
            if coalizao[key]:
                coalizao[key] = parse_date(coalizao[key])
        return coalizao

    def _get_coalizoes_partidos(self):
        path = os.path.join(db.DATA_PATH, 'tbl_CoalizaoP_X_Partido.csv')
        with open(path, 'r') as csv_file:
            reader = csv.DictReader(csv_file)
            result = {}
            for coalizao_partido in reader:
                id_coalizao = coalizao_partido["Id_Clz"]
                partido = coalizao_partido["Sigla_Partido"]
                result[id_coalizao] = result.get(id_coalizao, [])
                result[id_coalizao].append(partido)
            return result

    def _convert_to_dict(self, parlamentar):
        return collections.OrderedDict([
            ('id', parlamentar.parlamentar_id),
            ('name', parlamentar.parlamentar_nome),
            ('party', parlamentar.parlamentar_partido),
            ('rollcall_id', parlamentar.votacao_id),
            ('rollcall_date', parlamentar.votacao.data),
        ])
