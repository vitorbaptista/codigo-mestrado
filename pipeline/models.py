# -*- coding: utf-8 -*-

import csv
import json
import datetime
from sqlalchemy import Column, Integer, String, Date, DateTime, ForeignKey
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Proposicao(Base):
    __tablename__ = 'proposicoes'

    id = Column(Integer, primary_key=True)
    nome = Column(String)
    tipo = Column(String)
    tema = Column(String)
    ano = Column(Integer)
    numero = Column(Integer)
    regime_tramitacao = Column(String)
    autor = Column(String)
    autor_id = Column(Integer)
    autor_uf = Column(String)
    autor_partido = Column(String)
    ementa = Column(String)
    explicacao_ementa = Column(String)
    situacao = Column(String)
    indexacao = Column(String)
    apreciacao = Column(String)
    data_apresentacao = Column(Date)
    link_inteiro_teor = Column(String)
    ultimo_despacho = Column(String)
    data_ultimo_despacho = Column(Date)
    id_proposicao_principal = Column(Integer)
    nome_proposicao_origem = Column(String)

    @classmethod
    def create_from_csv(cls, proposicao_csv_path):
        with open(proposicao_csv_path, 'r') as proposicao_csv:
            proposicoes = [cls.build(p)
                           for p in csv.DictReader(proposicao_csv)]
            return proposicoes

    @classmethod
    def build(cls, proposicao_dict):
        return cls(
            id=_parse_int(proposicao_dict['id']),
            nome=proposicao_dict['nome'],
            tipo=proposicao_dict['tipo'],
            tema=proposicao_dict['tema'],
            ano=_parse_int(proposicao_dict['ano']),
            numero=_parse_int(proposicao_dict['numero']),
            regime_tramitacao=proposicao_dict['regime_tramitacao'],
            autor=proposicao_dict['autor'],
            autor_id=_parse_int(proposicao_dict['autor_id']),
            autor_uf=proposicao_dict['autor_uf'],
            autor_partido=proposicao_dict['autor_partido'],
            ementa=proposicao_dict['ementa'],
            explicacao_ementa=proposicao_dict['explicacao_ementa'],
            situacao=proposicao_dict['situacao'],
            indexacao=proposicao_dict['indexacao'],
            apreciacao=proposicao_dict['apreciacao'],
            data_apresentacao=cls._parse_date(proposicao_dict['data_apresentacao']),
            link_inteiro_teor=proposicao_dict['link_inteiro_teor'],
            ultimo_despacho=proposicao_dict['ultimo_despacho'],
            data_ultimo_despacho=cls._parse_date(proposicao_dict['ultimo_despacho_data']),
            id_proposicao_principal=_parse_int(proposicao_dict['id_proposicao_principal']),
            nome_proposicao_origem=_none_if_empty(proposicao_dict['nome_proposicao_origem']),
        )

    @staticmethod
    def _parse_date(date, date_format='%Y-%m-%d %H:%M:%S'):
        if date:
            return datetime.datetime.strptime(date, date_format)


class Votacao(Base):
    __tablename__ = 'votacoes'

    id = Column(Integer, primary_key=True)
    id_sessao = Column(Integer)
    data = Column(DateTime)
    obj_votacao = Column(String)
    resumo = Column(String)
    proposicao_id = Column(Integer, ForeignKey('proposicoes.id'))
    proposicao = relationship('Proposicao', backref=backref('votacoes',
                                                            order_by=id))

    @classmethod
    def create_from_file(cls, session, file_path):
        votacoes = []
        with open(file_path, 'r') as votacoes_json:
            votacoes_proposicoes = json.load(votacoes_json)
            for votacao_proposicao in votacoes_proposicoes:
                proposicao = session.query(Proposicao).\
                    filter_by(ano=int(votacao_proposicao['ano'])).\
                    filter_by(numero=int(votacao_proposicao['numero'])).\
                    filter_by(tipo=votacao_proposicao['sigla']).\
                    first()
                for votacao in votacao_proposicao.get('votacoes', []):
                    votacoes += [cls.build(proposicao, votacao)]
        return votacoes

    @classmethod
    def build(cls, proposicao, sessao_dict):
        datahora = '%s %s' % ((sessao_dict['data'], sessao_dict['hora']))
        votacao = cls(
            proposicao=proposicao,
            id_sessao=sessao_dict['cod_sessao'],
            data=cls._parse_date(datahora),
            obj_votacao=sessao_dict['obj_votacao'],
            resumo=_none_if_empty(sessao_dict['resumo']),
        )
        [Voto.build(votacao, voto)
         for voto in sessao_dict.get('votos', [])]
        [Orientacao.build(votacao, orientacao)
         for orientacao in sessao_dict.get('orientacao_bancada', [])]
        return votacao

    @staticmethod
    def _parse_date(date, date_format='%d/%m/%Y %H:%M'):
        if date:
            return datetime.datetime.strptime(date, date_format)


class Voto(Base):
    __tablename__ = 'votos'

    parlamentar_id = Column(Integer, index=True)
    parlamentar_nome = Column(String, primary_key=True)
    parlamentar_partido = Column(String, index=True)
    parlamentar_uf = Column(String(2), index=True)
    voto = Column(String, index=True)
    votacao_id = Column(Integer, ForeignKey('votacoes.id'), primary_key=True)
    votacao = relationship('Votacao', backref=backref('votos',
                                                      order_by=parlamentar_partido))

    @classmethod
    def build(cls, votacao, voto_dict):
        return cls(
            votacao=votacao,
            parlamentar_id=_parse_int(voto_dict.get('ide_cadastro')),
            parlamentar_nome=voto_dict['nome'],
            parlamentar_partido=voto_dict['partido'],
            parlamentar_uf=voto_dict['uf'],
            voto=voto_dict['voto'],
        )


class Orientacao(Base):
    __tablename__ = 'orientacoes'

    sigla = Column(String, primary_key=True)
    orientacao = Column(String, index=True)
    votacao_id = Column(Integer, ForeignKey('votacoes.id'), primary_key=True)
    votacao = relationship('Votacao', backref=backref('orientacao',
                                                      order_by=sigla))

    @classmethod
    def build(cls, votacao, orientacao_dict):
        return cls(
            votacao=votacao,
            sigla=orientacao_dict['sigla'],
            orientacao=orientacao_dict['orientacao'],
        )


def _none_if_empty(value):
    if value:
        return value


def _parse_int(value):
    if value:
        return int(value)
