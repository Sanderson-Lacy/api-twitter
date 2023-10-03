from psycopg2 import connect
from psycopg2 import Error as ErrorPostgres

class Conexao:
    def open_con(self):
        self.conexao = connect(host='db-sanderson.cmvjpmpr40an.us-east-1.rds.amazonaws.com',
                               user='sanderson', password='dbsandersonadmin', dbname='db_projeto')

    def close_con(self):
        self.conexao.close()

    def cursor(self):
        return self.conexao.cursor()  # retornando objeto cursor

    def commit(self):
        self.conexao.commit()

    def disable_autocommit(self):
        self.conexao.set_session(autocommit=False)

    def enable_autocommit(self):
        self.conexao.set_session(autocommit=True)

    def rollback(self):
        self.conexao.rollback()

class Usuario:
    def __init__(self):
        self.con = Conexao()

    def new_user_base(self, username, senha, email):
        self.con.open_con()
        self.con.enable_autocommit()
        cs = self.con.cursor()
        try:
            cs.execute(f"INSERT INTO projeto_twitter.usuario (username, senha, email) VALUES ('{username}', '{senha}',  ARRAY['{email}']);")
            return 200
        except (Exception, ErrorPostgres) as e:
            print(f'Erro: {e}')
            return 500
        finally:
            cs.close()
            self.con.close_con()

    def add_email(self, username, new_email):
        self.con.open_con()
        self.con.enable_autocommit()
        cs = self.con.cursor()
        try:
            cs.execute(f"UPDATE projeto_twitter.usuario SET email = ARRAY_APPEND(email, '{new_email}') WHERE username='{username}';")
            return 200
        except (Exception, ErrorPostgres) as e:
            print(f'Erro: {e}')
            return 500
        finally:
            cs.close()
            self.con.close_con()

    def add_nome(self, username, nome, sobrenome):
        self.con.open_con()
        self.con.enable_autocommit()
        cs = self.con.cursor()
        try:
            cs.execute(f"UPDATE projeto_twitter.usuario SET nome = '{nome}', sobrenome = '{sobrenome}' WHERE username='{username}';")
            return 200
        except (Exception, ErrorPostgres) as e:
            print(f'Erro: {e}')
            return 500
        finally:
            cs.close()
            self.con.close_con()

    def add_data_nascimento(self, username, dia, mes, ano):
        self.con.open_con()
        self.con.enable_autocommit()
        cs = self.con.cursor()
        try:
            cs.execute(f"UPDATE projeto_twitter.usuario SET data_nascimento = '{ano}-{mes}-{dia}' WHERE username='{username}';")
            return 200
        except (Exception, ErrorPostgres) as e:
            print(f'Erro: {e}')
            return 500
        finally:
            cs.close()
            self.con.close_con()

    def delete_user(self, username):
        self.con.open_con()
        self.con.enable_autocommit()
        cs = self.con.cursor()
        try:
            cs.execute(f"DELETE FROM projeto_twitter.usuario WHERE username = '{username}';")
            return 200
        except (Exception, ErrorPostgres) as e:
            print(f'Erro: {e}')
            return 500
        finally:
            cs.close()
            self.con.close_con()

    def select_user(self, username=None):
        self.con.open_con()
        cs = self.con.cursor()
        if username:
            try:
                cs.execute(f"SELECT * FROM projeto_twitter.usuario WHERE username='{username}';")
                return cs.fetchone()
            except (Exception, ErrorPostgres) as e:
                print(f'Erro: {e}')
                return 500
            finally:
                cs.close()
                self.con.close_con()
        else:
            try:
                cs.execute('SELECT * FROM projeto_twitter.usuario;')
                return cs.fetchall()
            except (Exception, ErrorPostgres) as e:
                print(f'Erro: {e}')
                return 500
            finally:
                cs.close()
                self.con.close_con()

class Conteudo:
    def __init__(self):
        self.con = Conexao()

    def new_tweet(self, username, value):
        self.con.open_con()
        self.con.enable_autocommit()
        cs = self.con.cursor()
        try:
            cs.execute(f"INSERT INTO projeto_twitter.conteudo (value_conteudo, tipo, username_possuidor) VALUES ('{value}', '0', '{username}');")
            return 200
        except (Exception, ErrorPostgres) as e:
            print(f'erro: {e}')
            return 500
        finally:
            cs.close()
            self.con.close_con()

    def new_tweet_with_image(self, username, value, caminho_imagem):#TRANSAÇÃO
        self.con.open_con()
        self.con.disable_autocommit()
        cs = self.con.cursor()
        try:
            id_imagem = Imagem(cs).new_imagem(caminho_imagem)
            print(id_imagem)
            cs.execute(f"INSERT INTO projeto_twitter.conteudo (username_possuidor, value_conteudo, tipo, id_imagem_conteudo) VALUES ('{username}','{value}', '0', {id_imagem});")
            return 200
        except (Exception, ErrorPostgres) as e:
            self.con.rollback()
            print(f'Erro: {e}')
            return 500
        else:
            self.con.commit()
        finally:
            cs.close()
            self.con.close_con()

    def curtir(self, username, id_conteudo):#TRANSAÇÃO
        self.con.open_con()
        self.con.disable_autocommit()
        cs = self.con.cursor()
        try:
            cs.execute(f"UPDATE projeto_twitter.conteudo SET n_curtidas = n_curtidas + 1 WHERE id_conteudo = {id_conteudo};")
            Curtida(cs).add_curtida(username, id_conteudo)
            self.con.commit()
            return 200
        except (Exception, ErrorPostgres) as e:
            self.con.rollback()
            print(f'Erro: {e}')
            return 500
        finally:
            cs.close()
            self.con.close_con()

    def descurtir(self, username, id_conteudo):#TRANSAÇÃO
        self.con.open_con()
        self.con.disable_autocommit()
        cs = self.con.cursor()
        try:
            cs.execute(f"UPDATE projeto_twitter.conteudo SET n_curtidas = n_curtidas - 1 WHERE id_conteudo = {id_conteudo};")
            Curtida(cs).delete_curtida(username, id_conteudo)
            self.con.commit()
            return 200
        except (Exception, ErrorPostgres) as e:
            self.con.rollback()
            print(f'Erro: {e}')
            return 500
            
        finally:
            cs.close()
            self.con.close_con()

    def delete_conteudo(self, id_conteudo):
        self.con.open_con()
        self.con.enable_autocommit()
        cs = self.con.cursor()
        try:
            cs.execute(f"DELETE FROM projeto_twitter.conteudo WHERE id_conteudo = '{id_conteudo}';")
            return 200
        except (Exception, ErrorPostgres) as e:
            print(f'erro: {e}')
            return 500
        finally:
            cs.close()
            self.con.close_con()

    def delete_conteudo_with_image(self, id_conteudo):#TRANSAÇÃO
        self.con.open_con()
        self.con.disable_autocommit()
        cs = self.con.cursor()
        try:
            cs.execute(f"SELECT id_imagem FROM projeto_twitter.conteudo WHERE id_conteudo = {id_conteudo};")
            id_imagem = cs.fetchone()
            Imagem(cs).delete_imagem(id_imagem)
            cs.execute(f"DELETE FROM projeto_twitter.conteudo WHERE id_conteudo = {id_conteudo};")
            self.con.commit()
            return 200
        except (Exception, ErrorPostgres) as e:
            self.con.rollback()
            print(f'Erro: {e}')
            return 500
        finally:
            cs.close()
            self.con.close_con()

    def select_tweet(self, username=None):
        if not username:
            self.con.open_con()
            self.con.enable_autocommit()
            cs = self.con.cursor()
            try:
                cs.execute(f"SELECT * FROM projeto_twitter.conteudo WHERE tipo = '0';")
                return cs.fetchall()
            except (Exception, ErrorPostgres) as e:
                print(f'Erro: {e}')
                return 500
            finally:
                cs.close()
                self.con.close_con()
        else:
            self.con.open_con()
            self.con.enable_autocommit()
            cs = self.con.cursor()
            try:
                cs.execute(f"SELECT * FROM projeto_twitter.conteudo WHERE (tipo = '0' AND username_possuidor = '{username}');")
                return cs.fetchall()
            except (Exception, ErrorPostgres) as e:
                print(f'Erro: {e}')
                return 500
            finally:
                cs.close()
                self.con.close_con()

# SEM ROTAS: CLASSE AUXILIAR
class Imagem:
    def __init__(self, cs=None):#com argumento de cursor
        self.cs = cs
        if not self.cs:
            self.con = Conexao()

    def new_imagem(self, caminho_imagem):
        if not self.cs:
            self.con.open_con()
            self.con.enable_autocommit()
            cs = self.con.cursor()
        else:
            cs = self.cs
        try:
            cs.execute(f"INSERT INTO projeto_twitter.imagem (caminho_imagem) VALUES ('{caminho_imagem}') RETURNING id_imagem;")
            id_imagem = str(cs.fetchone())[1]
            return id_imagem
        finally:
            if not self.cs:
                cs.close()
                self.con.close_con()

    def delete_imagem(self, id_imagem):
        if not self.cs:
            self.con.open_con()
            self.con.enable_autocommit()
            cs = self.con.cursor()
        else:
            cs = self.cs
        try:
            cs.execute(f"DELETE FROM projeto_twitter.imagem WHERE id_imagem = {id_imagem};")
        finally:
            if not self.cs:
                cs.close()
                self.con.close_con()

    def select_imagem(self, id_imagem=None):
        if not id_imagem:
            if not self.cs:
                self.con.open_con()
                self.con.enable_autocommit()
                cs = self.con.cursor()
            else:
                cs = self.cs
            try:
                cs.execute(f"SELECT * FROM projeto_twitter.imagem;")
                r = cs.fetchall()
                print(r)
            finally:
                if not self.cs:
                    cs.close()
                    self.con.close_con()
        else:
            if not self.cs:
                self.con.open_con()
                self.con.enable_autocommit()
                cs = self.con.cursor()
            else:
                cs = self.cs
            try:
                cs.execute(f"SELECT * FROM projeto_twitter.imagem WHERE id_imagem = {id_imagem};")
                r = cs.fetchone()
                print(r)
            finally:
                if not self.cs:
                    cs.close()
                    self.con.close_con()
    def update_imagem(self, id_imagem, new_caminho):
        if not self.cs:
            self.con.open_con()
            self.con.enable_autocommit()
            cs = self.con.cursor()
        else:
            cs = self.cs
        try:
            cs.execute(f"UPDATE projeto_twitter.imagem SET caminho_imagem = '{new_caminho}' WHERE id_imagem = {id_imagem};")
        finally:
            if not self.cs:
                cs.close()
                self.con.close_con()
# SEM ROTAS: CLASSE AUXILIAR
class Curtida:
    def __init__(self, cs=None):# argumento de cursor opcional
        self.cs = cs
        if not self.cs:# se não for passado cursor é estabelecida uma conexão exclusiva para a operação
            self.con = Conexao()

    def add_curtida(self, username, id_conteudo):
        if not self.cs:
            self.con.open_con()
            self.con.enable_autocommit()
            cs = self.con.cursor()
        else:
            cs = self.cs
        try:
            cs.execute(f"INSERT INTO projeto_twitter.curtida (username_curtiu, id_conteudo_curtido) VALUES ('{username}', {id_conteudo});")
        finally:
            if not self.cs:
                cs.close()
                self.con.close_con()

    def delete_curtida(self, username, id_conteudo):
        if not self.cs:
            self.con.open_con()
            self.con.enable_autocommit()
            cs = self.con.cursor()
        else:
            cs = self.cs
        try:
            cs.execute(f"DELETE FROM projeto_twitter.curtida WHERE (username_curtiu, id_conteudo_curtido) = ('{username}', {id_conteudo});")
        finally:
            if not self.cs:
                cs.close()
                self.con.close_con()

class Tag:
    def __init__(self):
        self.con = Conexao()

    def new_tag(self, value_tag):
        self.con.open_con()
        self.con.enable_autocommit()
        cs = self.con.cursor()
        try:
            cs.execute(f"INSERT INTO projeto_twitter.tag VALUES ('{value_tag}');")
            return 200
        except (Exception, ErrorPostgres) as e:
            print(f'Erro: {e}')
            return 500
        finally:
            cs.close()
            self.con.close_con()
    
    def topico_tag(self, tag, new_topico):
        self.con.open_con()
        self.con.enable_autocommit()
        cs = self.con.cursor()
        try:
            cs.execute(f"UPDATE projeto_twitter.tag SET topico_tag = '{new_topico}' WHERE tag = '{tag}';")
            return 200
        except (Exception, ErrorPostgres) as e:
            print(f'Erro: {e}')
            return 500
        finally:
            cs.close()
            self.con.close_con()

    def trending_tag(self, tag, is_trending):
        self.con.open_con()
        self.con.enable_autocommit()
        cs = self.con.cursor()
        try:
            cs.execute(f"UPDATE projeto_twitter.tag SET trending_tag = {is_trending} WHERE tag = '{tag}';")
            return 200
        except (Exception, ErrorPostgres) as e:
            print(f'Erro: {e}')
            return 500
        finally:
            cs.close()
            self.con.close_con()

    def select_tag(self, tag=None):
        if tag:
            self.con.open_con()
            self.con.enable_autocommit()
            cs = self.con.cursor()
            try:
                cs.execute(f"SELECT * FROM projeto_twitter.tag WHERE tag = '{tag}';")
                return cs.fetchone()
            except (Exception, ErrorPostgres) as e:
                print(f'Erro: {e}')
                return 500
            finally:
                cs.close()
                self.con.close_con()
        else:
            self.con.open_con()
            self.con.enable_autocommit()
            cs = self.con.cursor()
            try:
                cs.execute(f"SELECT * FROM projeto_twitter.tag;")
                return cs.fetchall()
            except (Exception, ErrorPostgres) as e:
                print(f'Erro: {e}')
                return 500
            finally:
                cs.close()
                self.con.close_con()
    
    def delete_tag(self, tag):
        self.con.open_con()
        self.con.enable_autocommit()
        cs = self.con.cursor()
        try:
            cs.execute(f"DELETE FROM projeto_twitter.tag WHERE tag = '{tag}';")
            return 200
        except (Exception, ErrorPostgres) as e:
            print(f'Erro: {e}')
            return 500
        finally:
            cs.close()
            self.con.close_con()