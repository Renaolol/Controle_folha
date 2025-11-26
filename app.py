from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from db import get_empresas, get_checklist, salvar_checklist, get_operacoes, add_operacao, delete_operacao, get_operacoes_vinculadas, salvar_vinculos, atualizar_ordem_operacoes, get_operacao_por_codigo, atualizar_operacao, get_comentario_empresa, salvar_comentario_empresa
app = Flask(__name__)
app.secret_key = "gcont123"
@app.route("/", methods=["GET", "POST"])
def index():
    empresas = get_empresas()
    if request.method == "POST":
        cod_emp = request.form.get("empresa")
        competencia = request.form.get("competencia")
        return redirect(url_for('exibir_checklist', cod_emp=cod_emp, competencia=competencia))
    return render_template("index.html", empresas=empresas)

# @app.route("/checklist/<cod_emp>/<competencia>", methods=["GET", "POST"])
# def checklist(cod_emp, competencia): 
#     if request.method == "POST":
#         dados = request.form.to_dict(flat=False)
#         salvar_checklist(cod_emp, competencia, dados)
#     checklist = get_checklist(cod_emp, competencia)
#     return render_template("checklist.html", checklist=checklist, cod_emp=cod_emp, competencia=competencia)

@app.route("/admin/operacoes", methods=["GET", "POST"])
def admin_operacoes():
    if request.method == "POST":
        codigo = request.form.get("codigo_operacao")
        descricao = request.form.get("descricao")
        if codigo and descricao:
            add_operacao(int(codigo), descricao)
            flash("Operação cadastrada com sucesso.")
            return redirect(url_for('admin_operacoes'))
    operacoes = get_operacoes()
    return render_template("admin_operacoes.html", operacoes=operacoes)

@app.route("/admin/operacoes/delete/<int:codigo>")
def excluir_operacao(codigo):
    delete_operacao(codigo)
    flash("Operação excluída.")
    return redirect(url_for("admin_operacoes"))

@app.route("/admin/vincular", methods=["GET", "POST"])
def vincular_operacoes():
    empresas = get_empresas()
    operacoes = get_operacoes()
    selecionada = request.args.get("empresa")
    selecionada_nome = None
    vinculadas = []

    if selecionada:
        selecionada_nome = next((e[1] for e in empresas if str(e[0]) == selecionada), None)
        vinculadas = get_operacoes_vinculadas(selecionada)

    if request.method == "POST":
        empresa_id = request.form.get("empresa_id")
        selecionadas = request.form.getlist("operacoes")
        salvar_vinculos(empresa_id, selecionadas)
        flash("Vínculos atualizados com sucesso.")
        return redirect(url_for("vincular_operacoes", empresa=empresa_id))

    return render_template("vincular_empresas.html",
                           empresas=empresas,
                           operacoes=operacoes,
                           selecionada=selecionada,
                           selecionada_nome=selecionada_nome,
                           vinculadas=vinculadas)

@app.route("/admin/operacoes/atualizar/<int:codigo>", methods=["POST"])
def atualizar_operacao_view(codigo):
    nova_desc = request.form.get("descricao")
    if nova_desc:
        atualizar_operacao(codigo, nova_desc)
        flash("Operação atualizada com sucesso.")
    return redirect(url_for("admin_operacoes"))

@app.route("/checklist/<cod_emp>/<competencia>", methods=["GET", "POST"])
def exibir_checklist(cod_emp, competencia):
    if request.method == "POST":
        if request.form.get("salvar_comentario") == "1":
            comentario = request.form.get("comentario_empresa", "")
            salvar_comentario_empresa(cod_emp, comentario)
            flash("Comentário salvo com sucesso.")
            return redirect(url_for("exibir_checklist", cod_emp=cod_emp, competencia=competencia))
        else:
            dados = request.form.to_dict(flat=False)
            salvar_checklist(cod_emp, competencia, dados)

    checklist = get_checklist(cod_emp, competencia)
    comentario = get_comentario_empresa(cod_emp)
    return render_template("checklist.html", checklist=checklist, cod_emp=cod_emp, competencia=competencia, comentario=comentario)

@app.route("/admin/operacoes/reordenar", methods=["POST"])
def reordenar_operacoes():
    ordem = request.json.get("ordem")
    if ordem:
        atualizar_ordem_operacoes(ordem)
        return jsonify({"status": "ok"})
    return jsonify({"status": "erro"}), 400

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5005, debug=True)
