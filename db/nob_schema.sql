-- SQL Server schema for NOB uploads and records
CREATE TABLE nob_uploads (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    user_email VARCHAR(255) NOT NULL,
    original_filename VARCHAR(255) NOT NULL,
    stored_filename VARCHAR(255) NOT NULL,
    output_filename VARCHAR(255) NULL,
    data_arquivo DATETIME NULL,
    uploaded_at DATETIME NOT NULL DEFAULT GETDATE()
);

CREATE TABLE nob (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    upload_id BIGINT NULL,
    exercicio VARCHAR(50) NULL,
    numero_nob VARCHAR(100) NULL,
    numero_nob_estorno VARCHAR(100) NULL,
    numero_liq VARCHAR(100) NULL,
    numero_emp VARCHAR(100) NULL,
    numero_ped VARCHAR(100) NULL,
    valor_nob DECIMAL(18, 2) NULL,
    devolucao_gcv DECIMAL(18, 2) NULL,
    valor_nob_gcv DECIMAL(18, 2) NULL,
    data_nob DATETIME NULL,
    data_cadastro_nob DATETIME NULL,
    data_hora_cadastro_liq DATETIME NULL,
    dotacao_orcamentaria VARCHAR(255) NULL,
    natureza_despesa VARCHAR(255) NULL,
    nome_fonte_recurso VARCHAR(255) NULL,
    ug VARCHAR(50) NULL,
    uo VARCHAR(50) NULL,
    nome_credor_principal VARCHAR(255) NULL,
    cpf_cnpj_credor_principal VARCHAR(50) NULL,
    credor VARCHAR(255) NULL,
    nome_credor VARCHAR(255) NULL,
    cpf_cnpj_credor VARCHAR(50) NULL,
    historico_liq NVARCHAR(MAX) NULL,
    empenho_atual VARCHAR(100) NULL,
    empenho_rp VARCHAR(100) NULL,
    funcao VARCHAR(50) NULL,
    subfuncao VARCHAR(50) NULL,
    programa_governo VARCHAR(100) NULL,
    paoe VARCHAR(100) NULL,
    cat_econ VARCHAR(50) NULL,
    grupo VARCHAR(50) NULL,
    modalidade VARCHAR(50) NULL,
    iduso VARCHAR(50) NULL,
    raw_payload NVARCHAR(MAX) NULL,
    data_atualizacao DATETIME NULL,
    data_arquivo DATETIME NULL,
    user_email VARCHAR(255) NULL,
    ativo BIT NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT GETDATE()
);

CREATE INDEX idx_nob_upload ON nob (upload_id);
CREATE INDEX idx_nob_ativo ON nob (ativo);
