# coding: utf-8

import MySQLdb
import sqlite3
import pandas as pd
import os
import re

# Configurando diretório do SQLite
DIR_SQLITE_DATABASE = 'db.sqlite3'

class ImportDataBase:

    def __init__(self):
        self.db = MySQLdb.connect(
            host='localhost',
            user='root',
            passwd='root',
            db='testesqlite'
        )
        self.cursor = self.db.cursor()

        self.dbsqlite = sqlite3.connect(DIR_SQLITE_DATABASE)
        self.cursorsqlite = self.dbsqlite.cursor()

    def importa_dados_sqlite_para_csv(self):
        # Realizando consulta no banco de dados sqlite para importar dados em
        # csv com pandas.

        # Consulta para pegar todos os nomes das tabelas.
        sql = """SELECT name FROM sqlite_master WHERE type='table';"""

        try:
            self.cursorsqlite = self.dbsqlite.cursor()
            self.cursorsqlite.execute(sql)
            ret = self.cursorsqlite.fetchall()
        except Exception as e:
            raise
        else:
            for tabela in ret:
                tabela = tabela[0]
                # Pegando todos os dados da tabela pesquisando com o nome que
                # foi encontrado.
                table = pd.read_sql_query("SELECT * FROM %s" % tabela, self.dbsqlite)
                table.to_csv('csv/'+ tabela + '.csv', index_label='index')
        finally:
            self.cursorsqlite.close()

    def lendo_arquivos_csv_importados_do_sqlite(self):
        # Pegando o diretório atual, e acrescentando a pasta dentro dela que se
        # chama csv
        diretorio = os.getcwd()
        diretorio = diretorio + '/csv/'

        # Loop no nome dos arquivos
        for subdiretorios, diretorios, arquivos in os.walk(diretorio):
            for arquivo in arquivos:
                tabela = pd.read_csv(diretorio+arquivo)
                nomeTabela = (re.split(r'[.]csv$', arquivo)[0])

                # Inserindo dados encontrados do arquivo no banco de dados
                self.inserindo_dados_csv_sqlite_para_mysql(nomeTabela, tabela)

    def inserindo_dados_csv_sqlite_para_mysql(self, nomeTabela, dados):
        colunas = list(dados.columns.values[1:])
        colunas = ', '.join(colunas)
        for valores in dados.values:
            inserir = list(valores[1:])
            inserir = str(inserir).replace('[', '')
            inserir = inserir.replace(']', '')
            sql = """INSERT INTO araujoshome.%s
                        (%s, user_id)
                        VALUES (%s, 1) """ % (nomeTabela, colunas, inserir)
            print(sql)
            try:
                self.cursor = self.db.cursor()
                self.cursor.execute(sql)
                self.db.commit()
            except Exception as e:
                raise
            else:
                print('Valor inserido %s' % inserir)
            finally:
                self.cursor.close()

    def verificando_existencia_pasta_csv(self):
        if not(os.path.exists('csv')):
            os.system('mkdir csv')

    def turn_the_motors_off(self):

        # Desligando os motores do banco de dados mysql.
        if self.cursor:
            self.cursor.close()
        if self.db:
            self.db.close()

        # Desligando os motores do banco de dados sqlite3.
        if self.cursorsqlite:
            self.cursorsqlite.close()
        if self.dbsqlite:
            self.dbsqlite.close()

idb = ImportDataBase()
idb.verificando_existencia_pasta_csv()
idb.importa_dados_sqlite_para_csv()
idb.turn_the_motors_off()
