import os
import pandas as pd
import sqlalchemy
from sqlalchemy import text
from dotenv import load_dotenv

from inserts import *

import mysql.connector

# Load the .env file
load_dotenv()

#Configuracao do database 
config = {
  'user': os.environ.get("USERNAME").strip(),
  'password': os.environ.get("PASSWORD").strip(),
  'host': os.environ.get("HOST").strip(),
  'database': os.environ.get("DATABASE").strip(),
}

cnx = mysql.connector.connect(
      host=config['host'],
      user=config['user'],
      password=config['password'],
      database=config['database']
)

df_2020 = pd.read_csv(f"{os.path.dirname(os.path.abspath(__file__))}/csv/2020_dataframe_normalized.csv", sep=";")

df_bairro_2020 = df_2020.filter(["RPA", 'BAIRRO'], axis=1)
df_bairro_2020 = df_bairro_2020.rename(columns={"RPA": "cod_rpa", "BAIRRO": "nome_bairro" })

#print(df_2020['COD_ANOENSINO'].unique())

print(df_2020.columns)

df_escola_2020 = df_2020.filter(["NOME_ESCOLA", "ENDERECO", 'BAIRRO'], axis=1)
df_escola_2020 = df_escola_2020.rename(columns={"NOME_ESCOLA": "nome_escola", "ENDERECO": "endereco", "BAIRRO": "nome_bairro" })

df_modalidade_2020 = df_2020.filter(["MODALIDADE", "COD_MODALIDADE", 'ANOENSINO', 'COD_ANOENSINO', 'TURMA', 'TURNO', 'NOME_ESCOLA'], axis=1)
df_modalidade_2020 = df_modalidade_2020.rename(columns={"MODALIDADE": "modalidade", "COD_MODALIDADE": "cod_modalidade", "ANOENSINO": "serie", "COD_ANOENSINO": "cod_serie", "TURMA": "turma", "TURNO": "turno", "NOME_ESCOLA": "nome_escola"})

df_aluno_2020 = df_2020.filter(["MATRICULA", "SEXO", 'IDADE'], axis=1)
df_aluno_2020 = df_aluno_2020.rename(columns={"MATRICULA": "num_matricula", "SEXO": "sexo", "IDADE": "idade" })

df_matricula_2020 = df_2020.filter(["NOME_ESCOLA", "MATRICULA", 'ANO_LETIVO', 'MODALIDADE', 'IDADE', 'ANOENSINO', 'TURMA', 'TURNO'], axis=1)
df_matricula_2020 = df_matricula_2020.rename(columns={"NOME_ESCOLA": "nome_escola", "MATRICULA": "num_matricula", "ANO_LETIVO": "ano_letivo", "MODALIDADE": "modalidade", "IDADE": "idade", "ANOENSINO": "serie", "TURMA": "turma", "TURNO": "turno"})


df_matricula_2020 = df_matricula_2020[df_matricula_2020['num_matricula'] == 13320432]
df_aluno_2020 = df_aluno_2020[df_aluno_2020['num_matricula'] == 13320432]

cursor = cnx.cursor()

print(df_2020.columns)

filtered_df = df_2020[df_2020['MATRICULA'] == 13320432]

print(df_aluno_2020.head())

#resetTables(cursor)
#insertBairro(df_bairro_2020.drop_duplicates(subset=['nome_bairro']), cursor)
#insertEscola(df_escola_2020.drop_duplicates(subset=['nome_escola']), cursor)
#insertModalidade(df_modalidade_2020.drop_duplicates(subset=['modalidade', 'serie', 'turma', 'turno']), cursor)
#insertAluno(df_aluno_2020.drop_duplicates(subset=['num_matricula', 'sexo', 'idade']), cursor)
#cursor.execute("DELETE FROM matricula;")
insertMatricula(df_matricula_2020.drop_duplicates(subset=['num_matricula', 'ano_letivo', 'idade', 'modalidade']), cursor)

cnx.commit()
cursor.close()
cnx.close()

#with engine.begin() as connection:
    #cursor.execute("SELECT * FROM aluno")
    #df_bairro_2020.to_sql(name='bairro', con=connection, if_exists='append', index_label='id_bairro')   


    #df_2020.to_sql(name='aluno', con=connection, if_exists='append')
    #df1 = pd.DataFrame({'sexo' : ['F', 'M'], 'num_matricula': ['1234', '4321'], 'idade': [10, 11]})
    #df_teste.to_sql(name='aluno', con=connection, if_exists='append', index_label='id_aluno')

#df_2020.to_sql(name="aluno", con=engine, if_exists='append')

#cnx.close()





