-- SQL Server schema for EST EMP uploads and records
CREATE TABLE est_emp_uploads (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    user_email VARCHAR(255) NOT NULL,
    original_filename VARCHAR(255) NOT NULL,
    stored_filename VARCHAR(255) NOT NULL,
    output_filename VARCHAR(255) NULL,
    data_arquivo DATETIME NULL,
    uploaded_at DATETIME NOT NULL DEFAULT GETDATE()
);

CREATE TABLE est_emp (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    upload_id BIGINT NULL,
    exercicio VARCHAR(50) NULL,
    numero_est VARCHAR(100) NULL,
    numero_emp VARCHAR(100) NULL,
    numero_ped VARCHAR(100) NULL,
    rp VARCHAR(50) NULL,
    situacao VARCHAR(255) NULL,
    historico NVARCHAR(MAX) NULL,
    valor_emp DECIMAL(18, 2) NULL,
    valor_est_emp_sem_aqs DECIMAL(18, 2) NULL,
    valor_est_emp_com_aqs DECIMAL(18, 2) NULL,
    valor_emp_liquido DECIMAL(18, 2) NULL,
    empenho_atual VARCHAR(100) NULL,
    empenho_rp VARCHAR(100) NULL,
    ug VARCHAR(50) NULL,
    uo VARCHAR(50) NULL,
    nome_unidade_orcamentaria VARCHAR(255) NULL,
    nome_unidade_gestora VARCHAR(255) NULL,
    dotacao_orcamentaria VARCHAR(255) NULL,
    credor VARCHAR(255) NULL,
    nome_credor VARCHAR(255) NULL,
    cpf_cnpj_credor VARCHAR(50) NULL,
    data_emissao DATETIME NULL,
    data_criacao DATETIME NULL,
    raw_payload NVARCHAR(MAX) NULL,
    data_atualizacao DATETIME NULL,
    data_arquivo DATETIME NULL,
    user_email VARCHAR(255) NULL,
    ativo BIT NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT GETDATE()
);

CREATE INDEX idx_est_emp_upload ON est_emp (upload_id);
CREATE INDEX idx_est_emp_ativo ON est_emp (ativo);
