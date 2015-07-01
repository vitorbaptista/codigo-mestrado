# -*- coding: utf-8 -*-

import os
import csv
import math
import collections
from datetime import datetime
from itertools import groupby

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
        intermediary_results = []
        for coalizao in coalizoes:
            partidos_na_coalizao = coalizoes_partidos[coalizao["Id_Clz"]]

            start_date = coalizao["DataInicial"]
            end_date = coalizao["DataFinal"]
            votos_coalizao = self._get_parlamentares_between(start_date,
                                                             end_date)

            intermediary_results += self._add_coalizao_column(votos_coalizao,
                                                              partidos_na_coalizao)

        results_legislatures = [self._get_legislature(x["rollcall_date"])
                                for x in intermediary_results]
        results = []
        for legislature in set(results_legislatures):
            start_index = results_legislatures.index(legislature)
            try:
                end_index = results_legislatures.index(legislature + 1)
            except ValueError:
                end_index = -1
            values = intermediary_results[start_index:end_index]
            if legislature == 54:
                import pdb
                pdb.set_trace()
            results += self._remove_uniques_and_convert_to_change_list(values)

        sort_keys = lambda v: (v["id"], v["rollcall_date"])
        return sorted(results, key=sort_keys)

    def _get_legislature(self, date):
        base_year = 1987
        base_legislature = 48
        years_since_base = date.year - base_year
        legislatures_since_base = years_since_base / 4.0
        fractional, _ = math.modf(legislatures_since_base)
        probable_legislature = base_legislature +\
            math.floor(legislatures_since_base)
        if fractional == 0 and date.month == 1:
            return probable_legislature - 1
        else:
            return probable_legislature

    def _remove_uniques_and_convert_to_change_list(self, rows):
        result = []
        get_id = lambda element: element["id"]
        get_rollcall_date = lambda element: element["rollcall_date"]
        rows = sorted(rows, key=get_rollcall_date)
        for _, elements in groupby(sorted(rows, key=get_id), get_id):
            elements = [r for r in elements]
            if len(elements) == 1:
                # Só estamos interessados em mudanças de coalizão, então
                # ignoramos quem nunca mudou
                continue
            for i in range(1, len(elements)):
                before = elements[i - 1]
                actual = elements[i]
                if before["coalizao"] == actual["coalizao"]:
                    continue
                result.append(collections.OrderedDict([
                    ("id", before["id"]),
                    ("name", before["name"]),
                    ("party_before", before["party"]),
                    ("party_after", actual["party"]),
                    ("rollcall_id", actual["rollcall_id"]),
                    ("rollcall_date", actual["rollcall_date"]),
                    ("coalition_before", before["coalizao"]),
                    ("coalition_after", actual["coalizao"]),
                ]))
        return result

    def _add_coalizao_column(self, votos_coalizao, partidos_coalizao):
        result = [collections.OrderedDict(v) for v in votos_coalizao]
        for voto in result:
            voto["coalizao"] = voto["party"] in partidos_coalizao
        return result

    def _get_parlamentares_between(self, start_date, end_date):
        between = models.Votacao.data.between(start_date,
                                              end_date)
        stmt = db.session.query(models.Voto)\
                 .join(models.Votacao)\
                 .filter(between)\
                 .filter(models.Voto.parlamentar_partido != 'S.Part.')\
                 .order_by(models.Voto.parlamentar_id)\
                 .order_by(models.Votacao.data.desc())\
                 .subquery()
        votos = db.session.query().add_entity(models.Voto, alias=stmt)\
                  .group_by(stmt.c.parlamentar_id)\
                  .group_by(stmt.c.parlamentar_partido)\
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
                partido = self._normalize_party_name(coalizao_partido["Sigla_Partido"])
                result[id_coalizao] = result.get(id_coalizao, [])
                result[id_coalizao].append(partido)
            return result

    def _normalize_party_name(self, party_name):
        normalizations = (
            {
                'names': ('pcb', 'pps'),
                'result': 'PCB>PPS'
            },
            {
                'names': ('pds', 'pp'),
                'result': 'PDS>PP'

            },
            {
                'names': ('pfl', 'dem'),
                'result': 'PFL>DEM'
            },
            {
                'names': ('pj', 'ptc'),
                'result': 'PJ>PTC'
            },
            {
                'names': ('pmr', 'prb'),
                'result': 'PMR>PRB'
            },
            {
                'names': ('pl', 'prona', 'pr'),
                'result': 'PL>PR'
            },
            {
                'names': ('sdd', 'solidaried'),
                'result': 'SD'
            },
            {
                'names': ('pcdob'),
                'result': 'PCdoB'
            }
        )

        for normalization in normalizations:
            if party_name.lower() in normalization['names']:
                return normalization['result']

        return party_name

    def _convert_to_dict(self, parlamentar):
        return collections.OrderedDict([
            ('id', parlamentar.parlamentar_id),
            ('name', parlamentar.parlamentar_nome),
            ('party', parlamentar.parlamentar_partido),
            ('rollcall_id', parlamentar.votacao_id),
            ('rollcall_date', parlamentar.votacao.data),
        ])
