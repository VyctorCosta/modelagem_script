import re

def resetTables(cursor):
    cursor.execute("DELETE FROM matricula;")
    cursor.execute("DELETE FROM modalidade;")
    cursor.execute("DELETE FROM escola;")
    cursor.execute("DELETE FROM bairro;")
    cursor.execute("DELETE FROM aluno;")

def insertBairro(df, cursor):
    print("len: ", len(df))
    total = len(df)
    x = 0.0
    for index, row in df.iterrows():
        y = round(index/total, 2)

        print("index: ", index)
        if index == 100:
            break

        #if y != x :
            #print(f"{(index/total) * 100}%")
        try:
            cursor.execute(f"INSERT INTO bairro (cod_rpa, nome_bairro) VALUES ({row['cod_rpa']}, '{row['nome_bairro']}');")
        except Exception as error:
            print("ERROR: ", error)
            if type(error).__name__ == 'IntegrityError':
                continue
            break
        

def insertEscola(df, cursor):
    for index, row in df.iterrows():
        if index == 100:
            break
        query = f"INSERT INTO escola (nome_escola, endereco, id_bairro) VALUES ('{row['nome_escola']}', '{row['endereco']}', (SELECT id_bairro FROM bairro where nome_bairro='{row['nome_bairro']}'));"
        print(query)
        try:
            cursor.execute(query)
        except Exception as error:
            print("ERROR: ", error)
            if type(error).__name__ == 'IntegrityError':
                continue
            break        
            

def insertModalidade(df, cursor):
    for index, row in df.iterrows():
        if index == 100:
            break
        
        modalidade = re.sub(r'\s+', ' ', row['modalidade']) # Removendo espaços internos maior para evitar conflito com o ENUM registrado
        query = f"INSERT INTO modalidade (modalidade, cod_modalidade, serie, cod_serie, turma, turno, id_escola) VALUES ('{modalidade}', {row['cod_modalidade']}, '{row['serie']}', {row['cod_serie']}, '{row['turma']}', '{row['turno']}', (SELECT id_escola FROM escola where nome_escola='{row['nome_escola']}'));"
        print(query)
        try:
            cursor.execute(query)
        except Exception as error:
            print("ERROR: ", error)
            if type(error).__name__ == 'IntegrityError':
                continue
            break


def insertAluno(df, cursor):
    for index, row in df.iterrows():
        if index == 100:
            break

        query = f"INSERT INTO aluno (num_matricula, sexo, idade) VALUES ({row['num_matricula']}, '{row['sexo']}', {row['idade']});"
        print(query)
        try:
            cursor.execute(query)
        except Exception as error:
            print("ERROR: ", error)
            if type(error).__name__ == 'IntegrityError':
                continue
            break

def insertMatricula(df, cursor):
    for index, row in df.iterrows():
        if index == 7200:
            break
        query = f"INSERT INTO matricula (id_escola, num_matricula, ano_letivo, id_modalidade) VALUES ((SELECT id_escola FROM escola where nome_escola='{row['nome_escola']}'), {row['num_matricula']}, {row['ano_letivo']}, (SELECT id_modalidade FROM modalidade where modalidade='{row['modalidade']}' AND serie='{row['serie']}' AND turma='{row['turma']}' AND turno='{row['turno']}'));"
        
        print(query)
        try:
            cursor.execute(query)
        except Exception as error:
            print("ERROR: ", error)
            if type(error).__name__ == 'IntegrityError':
                continue
            break
