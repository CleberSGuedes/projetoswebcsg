-- Parametros do sistema editaveis pelo Admin (ex.: tempo de inatividade da sessao).
-- O db.create_all() ja cria a tabela ao subir a aplicacao; este script serve para
-- criar manualmente (cPanel/phpMyAdmin) caso se prefira. Nao altera dados existentes.
CREATE TABLE IF NOT EXISTS configuracoes_sistema (
  chave VARCHAR(80) NOT NULL,
  valor VARCHAR(255) NOT NULL,
  atualizado_em DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  atualizado_por VARCHAR(255) NULL,
  PRIMARY KEY (chave)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
-- Sem linha = valor padrao do codigo (sessao_timeout_minutos = 120).
