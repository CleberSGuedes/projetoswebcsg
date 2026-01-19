from .db import db


class Usuario(db.Model):
    __tablename__ = "usuarios"

    id = db.Column(db.BigInteger)
    email = db.Column(db.String(255), primary_key=True)
    nome = db.Column(db.String(255), nullable=False)
    perfil = db.Column(db.String(50), nullable=False)
    ativo = db.Column(db.Boolean, nullable=False, default=True)
    password_hash = db.Column(db.String(255), nullable=False)

    def set_password(self, raw_password: str) -> None:
        from werkzeug.security import generate_password_hash

        self.password_hash = generate_password_hash(raw_password.strip())

    def check_password(self, raw_password: str) -> bool:
        from werkzeug.security import check_password_hash

        return check_password_hash(self.password_hash or "", raw_password or "")


class LogLogin(db.Model):
    __tablename__ = "logs_login"

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=False)
    email = db.Column(db.String(255), nullable=False)
    data_hora = db.Column(db.DateTime, server_default=db.func.now(), nullable=False)
    status = db.Column(db.String(20), nullable=False)
    motivo = db.Column(db.String(255))


class Perfil(db.Model):
    __tablename__ = "perfil"

    id = db.Column(db.Integer, primary_key=True, autoincrement=False)
    nome = db.Column(db.String(100), unique=True, nullable=False)
    nivel = db.Column(db.SmallInteger, nullable=False)
    ativo = db.Column(db.Boolean, nullable=False, default=True)
    criado_em = db.Column(db.DateTime, server_default=db.func.now(), nullable=False)
    atualizado_em = db.Column(
        db.DateTime, server_default=db.func.now(), onupdate=db.func.now(), nullable=False
    )


class ActiveSession(db.Model):
    __tablename__ = "active_sessions"

    # usa autoincrement para compatibilidade com MySQL
    id = db.Column(db.BigInteger, primary_key=True, autoincrement=False)
    email = db.Column(db.String(255), nullable=False)
    session_token = db.Column(db.String(64), nullable=False, unique=True)
    last_activity = db.Column(db.DateTime, nullable=False, server_default=db.func.now())


class PerfilPermissao(db.Model):
    __tablename__ = "perfil_permissoes"

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    perfil_id = db.Column(db.Integer, db.ForeignKey("perfil.id"), nullable=False)
    feature = db.Column(db.String(100), nullable=False)
    ativo = db.Column(db.Boolean, nullable=False, default=True, server_default=db.text("1"))
    created_at = db.Column(db.DateTime, nullable=False, server_default=db.func.now())
    updated_at = db.Column(db.DateTime, onupdate=db.func.now())


class NivelPermissao(db.Model):
    __tablename__ = "nivel_permissoes"

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    nivel = db.Column(db.SmallInteger, nullable=False)
    feature = db.Column(db.String(100), nullable=False)
    ativo = db.Column(db.Boolean, nullable=False, default=True, server_default=db.text("1"))
    created_at = db.Column(db.DateTime, nullable=False, server_default=db.func.now())
    updated_at = db.Column(db.DateTime, onupdate=db.func.now())


class Fip613Upload(db.Model):
    __tablename__ = "fip613_uploads"

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    user_email = db.Column(db.String(255), nullable=False)
    original_filename = db.Column(db.String(255), nullable=False)
    stored_filename = db.Column(db.String(255), nullable=False)
    output_filename = db.Column(db.String(255), nullable=True)
    data_arquivo = db.Column(db.DateTime, nullable=True)
    uploaded_at = db.Column(db.DateTime, nullable=False, server_default=db.func.now())


class Plan20Upload(db.Model):
    __tablename__ = "plan20_uploads"

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    user_email = db.Column(db.String(255), nullable=False)
    original_filename = db.Column(db.String(255), nullable=False)
    stored_filename = db.Column(db.String(255), nullable=False)
    output_filename = db.Column(db.String(255), nullable=True)
    data_arquivo = db.Column(db.DateTime, nullable=True)
    uploaded_at = db.Column(db.DateTime, nullable=False, server_default=db.func.now())


class Fip613Registro(db.Model):
    __tablename__ = "fip613"

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    upload_id = db.Column(db.BigInteger, nullable=True)
    uo = db.Column(db.String(50))
    ug = db.Column(db.String(50))
    funcao = db.Column(db.String(255))
    subfuncao = db.Column(db.String(255))
    programa = db.Column(db.String(255))
    projeto_atividade = db.Column(db.String(255))
    regional = db.Column(db.String(255))
    natureza_despesa = db.Column(db.String(255))
    fonte_recurso = db.Column(db.String(255))
    iduso = db.Column(db.Integer)
    tipo_recurso = db.Column(db.String(255))
    dotacao_inicial = db.Column(db.Numeric(18, 2))
    cred_suplementar = db.Column(db.Numeric(18, 2))
    cred_especial = db.Column(db.Numeric(18, 2))
    cred_extraordinario = db.Column(db.Numeric(18, 2))
    reducao = db.Column(db.Numeric(18, 2))
    cred_autorizado = db.Column(db.Numeric(18, 2))
    bloqueado_conting = db.Column(db.Numeric(18, 2))
    reserva_empenho = db.Column(db.Numeric(18, 2))
    saldo_destaque = db.Column(db.Numeric(18, 2))
    saldo_dotacao = db.Column(db.Numeric(18, 2))
    empenhado = db.Column(db.Numeric(18, 2))
    liquidado = db.Column(db.Numeric(18, 2))
    a_liquidar = db.Column(db.Numeric(18, 2))
    valor_pago = db.Column(db.Numeric(18, 2))
    valor_a_pagar = db.Column(db.Numeric(18, 2))
    data_atualizacao = db.Column(db.DateTime)
    ano = db.Column(db.Integer)
    data_arquivo = db.Column(db.DateTime)
    user_email = db.Column(db.String(255))
    ativo = db.Column(db.Boolean, nullable=False, default=True)
    created_at = db.Column(db.DateTime, nullable=False, server_default=db.func.now())


class PedUpload(db.Model):
    __tablename__ = "ped_uploads"

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    user_email = db.Column(db.String(255), nullable=False)
    original_filename = db.Column(db.String(255), nullable=False)
    stored_filename = db.Column(db.String(255), nullable=False)
    output_filename = db.Column(db.String(255), nullable=True)
    data_arquivo = db.Column(db.DateTime, nullable=True)
    uploaded_at = db.Column(db.DateTime, nullable=False, server_default=db.func.now())


class PedRegistro(db.Model):
    __tablename__ = "ped"

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    upload_id = db.Column(db.BigInteger, nullable=True)
    chave = db.Column(db.String(255))
    regiao = db.Column(db.String(255))
    subfuncao_ug = db.Column(db.String(255))
    adj = db.Column(db.String(255))
    macropolitica = db.Column(db.String(255))
    pilar = db.Column(db.String(255))
    eixo = db.Column(db.String(255))
    politica_decreto = db.Column(db.String(255))
    exercicio = db.Column(db.String(50))
    historico = db.Column(db.Text)
    numero_ped = db.Column(db.String(100))
    numero_ped_estorno = db.Column(db.String(100))
    numero_emp = db.Column(db.String(100))
    numero_cad = db.Column(db.String(100))
    numero_noblist = db.Column(db.String(100))
    numero_os = db.Column(db.String(100))
    convenio = db.Column(db.String(255))
    indicativo_licitacao_exercicios_anteriores = db.Column(db.String(255))
    liberado_fisco_estadual = db.Column(db.String(255))
    situacao = db.Column(db.String(255))
    uo = db.Column(db.String(100))
    nome_unidade_orcamentaria = db.Column(db.String(255))
    ug = db.Column(db.String(100))
    nome_unidade_gestora = db.Column(db.String(255))
    numero_processo_orcamentario_pagamento = db.Column(db.String(255))
    dotacao_orcamentaria = db.Column(db.String(255))
    funcao = db.Column(db.String(255))
    subfuncao = db.Column(db.String(255))
    programa_governo = db.Column(db.String(255))
    paoe = db.Column(db.String(255))
    natureza_despesa = db.Column(db.String(255))
    cat_econ = db.Column(db.String(50))
    grupo = db.Column(db.String(50))
    modalidade = db.Column(db.String(50))
    fonte = db.Column(db.String(50))
    iduso = db.Column(db.String(50))
    elemento = db.Column(db.String(100))
    nome_elemento = db.Column(db.String(255))
    numero_emenda_ep = db.Column(db.String(100))
    autor_emenda_ep = db.Column(db.String(255))
    numero_cac = db.Column(db.String(100))
    licitacao = db.Column(db.String(255))
    usuario_responsavel = db.Column(db.String(255))
    data_solicitacao = db.Column(db.String(50))
    data_criacao = db.Column(db.String(50))
    data_autorizacao = db.Column(db.String(50))
    data_licitacao = db.Column(db.String(50))
    data_hora_cadastro_autorizacao = db.Column(db.String(50))
    tipo_empenho = db.Column(db.String(100))
    tipo_despesa = db.Column(db.String(255))
    numero_abj = db.Column(db.String(100))
    numero_processo_sequestro_judicial = db.Column(db.String(255))
    indicativo_entrega_imediata = db.Column(db.String(255))
    indicativo_contrato = db.Column(db.String(255))
    codigo_uo_extinta = db.Column(db.String(100))
    devolucao_gcv = db.Column(db.String(255))
    mes_competencia_folha_pagamento = db.Column(db.String(50))
    exercicio_competencia_folha = db.Column(db.String(50))
    obrigacao_patronal = db.Column(db.String(255))
    tipo_obrigacao_patronal = db.Column(db.String(255))
    numero_nla = db.Column(db.String(100))
    valor_ped = db.Column(db.String(50))
    valor_estorno = db.Column(db.String(50))
    credor = db.Column(db.String(255))
    nome_credor = db.Column(db.String(255))
    chave_planejamento = db.Column(db.String(255))
    data_atualizacao = db.Column(db.DateTime)
    data_arquivo = db.Column(db.DateTime)
    user_email = db.Column(db.String(255))
    ativo = db.Column(db.Boolean, nullable=False, default=True, server_default=db.text("1"))
    created_at = db.Column(db.DateTime, nullable=False, server_default=db.func.now())


class EmpUpload(db.Model):
    __tablename__ = "emp_uploads"

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    user_email = db.Column(db.String(255), nullable=False)
    original_filename = db.Column(db.String(255), nullable=False)
    stored_filename = db.Column(db.String(255), nullable=False)
    output_filename = db.Column(db.String(255), nullable=True)
    data_arquivo = db.Column(db.DateTime, nullable=True)
    uploaded_at = db.Column(db.DateTime, nullable=False, server_default=db.func.now())


class EmpRegistro(db.Model):
    __tablename__ = "emp"

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    upload_id = db.Column(db.BigInteger, nullable=True)
    chave = db.Column(db.String(255))
    chave_planejamento = db.Column(db.String(255))
    regiao = db.Column(db.String(255))
    subfuncao_ug = db.Column(db.String(255))
    adj = db.Column(db.String(255))
    macropolitica = db.Column(db.String(255))
    pilar = db.Column(db.String(255))
    eixo = db.Column(db.String(255))
    politica_decreto = db.Column(db.String(255))
    exercicio = db.Column(db.String(50))
    situacao = db.Column(db.String(255))
    historico = db.Column(db.Text)
    numero_emp = db.Column(db.String(100))
    numero_ped = db.Column(db.String(100))
    numero_contrato = db.Column(db.String(100))
    numero_convenio = db.Column(db.String(100))
    dotacao_orcamentaria = db.Column(db.String(255))
    funcao = db.Column(db.String(255))
    subfuncao = db.Column(db.String(255))
    programa_governo = db.Column(db.String(255))
    paoe = db.Column(db.String(255))
    natureza_despesa = db.Column(db.String(255))
    cat_econ = db.Column(db.String(50))
    grupo = db.Column(db.String(50))
    modalidade = db.Column(db.String(50))
    fonte = db.Column(db.String(50))
    iduso = db.Column(db.String(50))
    elemento = db.Column(db.String(100))
    uo = db.Column(db.String(100))
    nome_unidade_orcamentaria = db.Column(db.String(255))
    ug = db.Column(db.String(100))
    nome_unidade_gestora = db.Column(db.String(255))
    data_emissao = db.Column(db.DateTime)
    data_criacao = db.Column(db.DateTime)
    valor_emp = db.Column(db.Numeric(18, 2))
    devolucao_gcv = db.Column(db.Numeric(18, 2))
    valor_emp_devolucao_gcv = db.Column(db.Numeric(18, 2))
    tipo_empenho = db.Column(db.String(100))
    tipo_despesa = db.Column(db.String(255))
    credor = db.Column(db.String(255))
    nome_credor = db.Column(db.String(255))
    cpf_cnpj_credor = db.Column(db.String(50))
    categoria_credor = db.Column(db.String(100))
    raw_payload = db.Column(db.Text)
    data_atualizacao = db.Column(db.DateTime)
    data_arquivo = db.Column(db.DateTime)
    user_email = db.Column(db.String(255))
    ativo = db.Column(db.Boolean, nullable=False, default=True, server_default=db.text("1"))
    created_at = db.Column(db.DateTime, nullable=False, server_default=db.func.now())


class EstEmpUpload(db.Model):
    __tablename__ = "est_emp_uploads"

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    user_email = db.Column(db.String(255), nullable=False)
    original_filename = db.Column(db.String(255), nullable=False)
    stored_filename = db.Column(db.String(255), nullable=False)
    output_filename = db.Column(db.String(255), nullable=True)
    data_arquivo = db.Column(db.DateTime, nullable=True)
    uploaded_at = db.Column(db.DateTime, nullable=False, server_default=db.func.now())


class EstEmpRegistro(db.Model):
    __tablename__ = "est_emp"

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    upload_id = db.Column(db.BigInteger, nullable=True)
    exercicio = db.Column(db.String(50))
    numero_est = db.Column(db.String(100))
    numero_emp = db.Column(db.String(100))
    numero_ped = db.Column(db.String(100))
    rp = db.Column(db.String(50))
    situacao = db.Column(db.String(255))
    historico = db.Column(db.Text)
    valor_emp = db.Column(db.Numeric(18, 2))
    valor_est_emp_sem_aqs = db.Column(db.Numeric(18, 2))
    valor_est_emp_com_aqs = db.Column(db.Numeric(18, 2))
    valor_emp_liquido = db.Column(db.Numeric(18, 2))
    empenho_atual = db.Column(db.String(100))
    empenho_rp = db.Column(db.String(100))
    ug = db.Column(db.String(50))
    uo = db.Column(db.String(50))
    nome_unidade_orcamentaria = db.Column(db.String(255))
    nome_unidade_gestora = db.Column(db.String(255))
    dotacao_orcamentaria = db.Column(db.String(255))
    credor = db.Column(db.String(255))
    nome_credor = db.Column(db.String(255))
    cpf_cnpj_credor = db.Column(db.String(50))
    data_emissao = db.Column(db.DateTime)
    data_criacao = db.Column(db.DateTime)
    raw_payload = db.Column(db.Text)
    data_atualizacao = db.Column(db.DateTime)
    data_arquivo = db.Column(db.DateTime)
    user_email = db.Column(db.String(255))
    ativo = db.Column(db.Boolean, nullable=False, default=True, server_default=db.text("1"))
    created_at = db.Column(db.DateTime, nullable=False, server_default=db.func.now())


class NobUpload(db.Model):
    __tablename__ = "nob_uploads"

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    user_email = db.Column(db.String(255), nullable=False)
    original_filename = db.Column(db.String(255), nullable=False)
    stored_filename = db.Column(db.String(255), nullable=False)
    output_filename = db.Column(db.String(255), nullable=True)
    data_arquivo = db.Column(db.DateTime, nullable=True)
    uploaded_at = db.Column(db.DateTime, nullable=False, server_default=db.func.now())


class NobRegistro(db.Model):
    __tablename__ = "nob"

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    upload_id = db.Column(db.BigInteger, nullable=True)
    exercicio = db.Column(db.String(50))
    numero_nob = db.Column(db.String(100))
    numero_nob_estorno = db.Column(db.String(100))
    numero_liq = db.Column(db.String(100))
    numero_emp = db.Column(db.String(100))
    numero_ped = db.Column(db.String(100))
    valor_nob = db.Column(db.Numeric(18, 2))
    devolucao_gcv = db.Column(db.Numeric(18, 2))
    valor_nob_gcv = db.Column(db.Numeric(18, 2))
    data_nob = db.Column(db.DateTime)
    data_cadastro_nob = db.Column(db.DateTime)
    data_hora_cadastro_liq = db.Column(db.DateTime)
    dotacao_orcamentaria = db.Column(db.String(255))
    natureza_despesa = db.Column(db.String(255))
    nome_fonte_recurso = db.Column(db.String(255))
    ug = db.Column(db.String(50))
    uo = db.Column(db.String(50))
    nome_credor_principal = db.Column(db.String(255))
    cpf_cnpj_credor_principal = db.Column(db.String(50))
    credor = db.Column(db.String(255))
    nome_credor = db.Column(db.String(255))
    cpf_cnpj_credor = db.Column(db.String(50))
    historico_liq = db.Column(db.Text)
    empenho_atual = db.Column(db.String(100))
    empenho_rp = db.Column(db.String(100))
    funcao = db.Column(db.String(50))
    subfuncao = db.Column(db.String(50))
    programa_governo = db.Column(db.String(100))
    paoe = db.Column(db.String(100))
    cat_econ = db.Column(db.String(50))
    grupo = db.Column(db.String(50))
    modalidade = db.Column(db.String(50))
    iduso = db.Column(db.String(50))
    raw_payload = db.Column(db.Text)
    data_atualizacao = db.Column(db.DateTime)
    data_arquivo = db.Column(db.DateTime)
    user_email = db.Column(db.String(255))
    ativo = db.Column(db.Boolean, nullable=False, default=True, server_default=db.text("1"))
    created_at = db.Column(db.DateTime, nullable=False, server_default=db.func.now())


class Plan21Nger(db.Model):
    __tablename__ = "plan21_nger"

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    exercicio = db.Column(db.String(50))
    chave_planejamento = db.Column(db.String(255))
    uo = db.Column("unidade_orcamentaria", db.String(255))
    programa = db.Column(db.String(255))
    acao_paoe = db.Column(db.String(255))
    produto = db.Column("produto_acao", db.String(255))
    ug = db.Column("unid_gestora", db.String(255))
    regiao = db.Column(db.String(255))
    subacao_entrega = db.Column(db.String(255))
    etapa = db.Column(db.String(255))
    elemento = db.Column(db.String(50))
    subelemento = db.Column(db.String(50))
    natureza = db.Column(db.String(255))
    fonte = db.Column(db.String(50))
    idu = db.Column(db.String(50))
    valor_atual = db.Column(db.Numeric(18, 2))
    ativo = db.Column(db.Boolean)


class Adj(db.Model):
    __tablename__ = "adj"

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    abreviacao = db.Column(db.String(100))
    ativo = db.Column(db.Boolean)


class Dotacao(db.Model):
    __tablename__ = "dotacao"

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    plan21_nger_id = db.Column(db.BigInteger)
    exercicio = db.Column(db.String(50))
    adj_id = db.Column(db.BigInteger)
    chave_planejamento = db.Column(db.String(255))
    uo = db.Column(db.String(50))
    programa = db.Column(db.String(255))
    acao_paoe = db.Column(db.String(255))
    produto = db.Column(db.String(255))
    ug = db.Column(db.String(50))
    regiao = db.Column(db.String(255))
    subacao_entrega = db.Column(db.String(255))
    etapa = db.Column(db.String(255))
    natureza_despesa = db.Column(db.String(255))
    elemento = db.Column(db.Integer)
    subelemento = db.Column(db.String(50))
    fonte = db.Column(db.String(50))
    iduso = db.Column(db.String(50))
    valor_dotacao = db.Column(db.Numeric(18, 2))
    valor_ped_emp = db.Column(db.Numeric(18, 2))
    valor_atual = db.Column(db.Numeric(18, 2))
    chave_dotacao = db.Column(db.String(255))
    justificativa_historico = db.Column(db.Text)
    usuarios_id = db.Column(db.BigInteger)
    criado_em = db.Column(db.DateTime, server_default=db.func.now())
    alterado_em = db.Column(db.DateTime)
    excluido_em = db.Column(db.DateTime)
    ativo = db.Column(db.Boolean, nullable=False, default=True, server_default=db.text("1"))
