import pyodbc
import psycopg2
from psycopg2.extras import RealDictCursor

# Conexão com banco da Domínio via ODBC
def get_odbc_conn():
    return pyodbc.connect("DSN=ContabilPBI;UID=PBI;PWD=Pbi")
# Conexão com PostgreSQL
def get_pg_conn():
    return psycopg2.connect(
        host="localhost",
        database="Checklist",
        user="postgres",
        password="0176"
    )

def get_empresas():
    conn = get_odbc_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT CODIGO, NOME FROM bethadba.PRVCLIENTES ORDER BY NOME")
    empresas = cursor.fetchall()
    conn.close()
    return empresas

def get_checklist(cod_emp, competencia):
    conn = get_pg_conn()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    # Busca operações vinculadas à empresa
    cursor.execute("""
        SELECT o.codigo_operacao, o.descricao
          FROM checklist_operacoes o
          JOIN checklist_emp_operacoes eo ON eo.codigo_operacao = o.codigo_operacao
         WHERE eo.cod_emp = %s
         ORDER BY o.ordem NULLS LAST
    """, (cod_emp,))
    operacoes = cursor.fetchall()
    checklist = []

    # Conexão ODBC paralelo
    odbc_conn = get_odbc_conn()
    odbc_cursor = odbc_conn.cursor()

    # Formata competências
    mes, ano = map(int, competencia.replace("-", "/").split("/"))
    competencia_fmt = f"{ano}{mes:02}"
    mes_ant = mes - 1 if mes > 1 else 12
    ano_ant = ano if mes > 1 else ano - 1
    periodo_inicio_fmt = f"{ano_ant}{mes_ant:02}"

    def get_val(query, params):
        """Retorna valor numérico ou 0 se não existir."""
        odbc_cursor.execute(query, params)
        r = odbc_cursor.fetchone()
        return r[0] if r and r[0] is not None else 0

    for op in operacoes:
        # Status salvo
        cursor.execute("""
            SELECT realizado, observacao
              FROM checklist_status
             WHERE cod_emp = %s
               AND competencia = %s
               AND codigo_operacao = %s
        """, (cod_emp, competencia, op['codigo_operacao']))
        status = cursor.fetchone()
        realizado = status['realizado'] if status else False
        observacao = status['observacao'] if status else ""

        if not observacao:
            cod = op['codigo_operacao']

            if cod == 10:
                val = get_val(
                    "SELECT total_guia FROM bethadba.foguiainss WHERE codi_emp = ? AND competencia = ? AND tipo_process = 11",
                    (cod_emp, competencia_fmt)
                )
                observacao = f"R$ {val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

            elif cod == 11:
                val = get_val(
                    "SELECT valor FROM bethadba.focalcirrf WHERE codi_emp = ? AND periodo_inicio = ?",
                    (cod_emp, competencia_fmt)
                )
                observacao = f"R$ {val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

            elif cod == 12:
                val = get_val(
                    "SELECT SUM(total_fgts) FROM bethadba.fofgtsfilial WHERE codi_emp = ? AND competencia = ?",
                    (cod_emp, competencia_fmt)
                )
                observacao = f"R$ {val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

            elif cod == 13:
                val = get_val(
                    "SELECT VALOR FROM bethadba.FOCONTRIBUICAO_SINDICAL WHERE CODI_EMP = ? AND COMPETENCIA = ?",
                    (cod_emp, competencia_fmt)
                )
                observacao = f"R$ {val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

            elif cod == 14:
                total_sdev = get_val(
                    "SELECT SUM(sdev_sim) FROM bethadba.efsdoimp "
                    "WHERE CODI_EMP = ? AND data_sim = ? AND codi_imp IN (16,18,103)",
                    (cod_emp, competencia_fmt)
                )
                observacao = f"R$ {total_sdev:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

            elif cod == 15:
                val = get_val(
                    "SELECT vdi2_sim FROM bethadba.efsdoimp "
                    "WHERE CODI_EMP = ? AND data_sim = ? AND codi_imp = 26",
                    (cod_emp, competencia_fmt)
                )
                observacao = f"R$ {val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

            elif cod == 46:
                val = get_val(
                    "SELECT SALDO_RECOLHER FROM bethadba.GEGUIA_CONTRIBUICAO_PREVIDENCIARIA_SALDOS_RECOLHER "
                    "WHERE CODI_EMP = ? AND COMPETENCIA = ? AND TIPO_SALDO_RECOLHER = 18",
                    (cod_emp, competencia_fmt)
                )
                observacao = f"R$ {val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

            elif cod == 47:
                # operação 47 = 10 + 11 + 46 − 15
                val10 = get_val(
                    "SELECT total_guia FROM bethadba.foguiainss WHERE codi_emp = ? AND competencia = ?",
                    (cod_emp, competencia_fmt)
                )
                val11 = get_val(
                    "SELECT valor FROM bethadba.focalcirrf WHERE codi_emp = ? AND periodo_inicio = ?",
                    (cod_emp, competencia_fmt)
                )
                val46 = get_val(
                    "SELECT SALDO_RECOLHER FROM bethadba.GEGUIA_CONTRIBUICAO_PREVIDENCIARIA_SALDOS_RECOLHER "
                    "WHERE CODI_EMP = ? AND COMPETENCIA = ? AND TIPO_SALDO_RECOLHER = 18",
                    (cod_emp, competencia_fmt)
                )
                val15 = get_val(
                    "SELECT vdi2_sim FROM bethadba.efsdoimp "
                    "WHERE CODI_EMP = ? AND data_sim = ? AND codi_imp = 26",
                    (cod_emp, competencia_fmt)
                )
                total = val10 + val11 + val46 - val15
                observacao = f"R$ {total:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

        checklist.append({
            "codigo_operacao": op['codigo_operacao'],
            "descricao":         op['descricao'],
            "realizado":         realizado,
            "observacao":        observacao
        })

    conn.close()
    odbc_conn.close()
    return checklist

def salvar_checklist(cod_emp, competencia, dados):
    conn = get_pg_conn()
    cursor = conn.cursor()
    for key in dados:
        if key.startswith("check_"):
            codigo = int(key.split("_")[1])
            realizado = True
            observacao = dados.get(f"obs_{codigo}", [""])[0]
            cursor.execute("""
                INSERT INTO checklist_status (cod_emp, competencia, codigo_operacao, realizado, observacao)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (cod_emp, competencia, codigo_operacao)
                DO UPDATE SET realizado = EXCLUDED.realizado, observacao = EXCLUDED.observacao
            """, (cod_emp, competencia, codigo, realizado, observacao))
    conn.commit()
    conn.close()

def get_operacoes():
    conn = get_pg_conn()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT * FROM checklist_operacoes ORDER BY ordem NULLS LAST")
    ops = cursor.fetchall()
    conn.close()
    return ops

def add_operacao(codigo_operacao, descricao):
    conn = get_pg_conn()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO checklist_operacoes (codigo_operacao, descricao)
        VALUES (%s, %s)
        ON CONFLICT (codigo_operacao) DO NOTHING
    """, (codigo_operacao, descricao))
    conn.commit()
    conn.close()

def delete_operacao(codigo_operacao):
    conn = get_pg_conn()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM checklist_operacoes WHERE codigo_operacao = %s", (codigo_operacao,))
    conn.commit()
    conn.close()

def get_operacoes_vinculadas(cod_emp):
    conn = get_pg_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT codigo_operacao FROM checklist_emp_operacoes WHERE cod_emp = %s
    """, (cod_emp,))
    vinculadas = [str(row[0]) for row in cursor.fetchall()]
    conn.close()
    return vinculadas

def salvar_vinculos(cod_emp, lista_operacoes):
    conn = get_pg_conn()
    cursor = conn.cursor()

    # Apaga os vínculos antigos
    cursor.execute("DELETE FROM checklist_emp_operacoes WHERE cod_emp = %s", (cod_emp,))

    # Insere os novos vínculos
    for op in lista_operacoes:
        cursor.execute("""
            INSERT INTO checklist_emp_operacoes (cod_emp, codigo_operacao)
            VALUES (%s, %s)
        """, (cod_emp, op))

    conn.commit()
    conn.close()

def get_operacao_por_codigo(codigo_operacao):
    conn = get_pg_conn()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT * FROM checklist_operacoes WHERE codigo_operacao = %s", (codigo_operacao,))
    op = cursor.fetchone()
    conn.close()
    return op

def atualizar_operacao(codigo_operacao, nova_descricao):
    conn = get_pg_conn()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE checklist_operacoes SET descricao = %s WHERE codigo_operacao = %s
    """, (nova_descricao, codigo_operacao))
    conn.commit()
    conn.close()

def get_comentario_empresa(cod_emp):
    conn = get_pg_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT comentario FROM checklist_comentarios WHERE cod_emp = %s", (cod_emp,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else ""

def salvar_comentario_empresa(cod_emp, texto):
    conn = get_pg_conn()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO checklist_comentarios (cod_emp, comentario)
        VALUES (%s, %s)
        ON CONFLICT (cod_emp)
        DO UPDATE SET comentario = EXCLUDED.comentario
    """, (cod_emp, texto))
    
    conn.commit()
    conn.close()

def atualizar_ordem_operacoes(lista_ordenada_ids):
    conn = get_pg_conn()
    cursor = conn.cursor()
    for ordem, codigo in enumerate(lista_ordenada_ids, start=1):
        cursor.execute("UPDATE checklist_operacoes SET ordem = %s WHERE codigo_operacao = %s", (ordem, codigo))
    conn.commit()
    conn.close()
