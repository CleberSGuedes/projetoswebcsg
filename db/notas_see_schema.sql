-- Notas SEE (Área UENs > SAGE): ajustes nas tabelas see_* (MariaDB 10.11).
--
-- As tabelas já existiam vazias no banco (criadas em 02/10/2026 por uma versão antiga do
-- modelo): o db.create_all() não altera tabela existente, então este script traz o esquema
-- ao que o código espera. Seguro porque as tabelas estão VAZIAS (conferir antes:
-- SELECT COUNT(*) em see_catalogos, see_processamentos, see_escolas).
--
-- 1) Exercício no catálogo; o nome passa a ser único DENTRO do exercício.
ALTER TABLE see_catalogos ADD COLUMN IF NOT EXISTS exercicio SMALLINT NOT NULL AFTER nome;
-- O índice único antigo só por nome pode ter um destes nomes, conforme a versão do modelo que
-- criou a tabela; apaga os dois (IF EXISTS: seguro rodar de novo).
ALTER TABLE see_catalogos DROP INDEX IF EXISTS nome;
ALTER TABLE see_catalogos DROP INDEX IF EXISTS uq_see_catalogo_nome;
ALTER TABLE see_catalogos ADD UNIQUE INDEX IF NOT EXISTS uq_see_catalogo_exercicio_nome (exercicio, nome);

-- 2) Índices de consulta (o modelo os declara; create_all não os cria em tabela existente).
ALTER TABLE see_processamentos ADD INDEX IF NOT EXISTS ix_see_processamentos_catalogo_status (catalogo_id, status);
ALTER TABLE see_processamento_arquivos ADD INDEX IF NOT EXISTS ix_see_proc_arq_proc_sha (processamento_id, sha256);
ALTER TABLE see_ocorrencias ADD INDEX IF NOT EXISTS ix_see_ocorr_proc_codigo (processamento_id, codigo);

-- (see_itens_extraidos e see_processamento_eventos já têm índice nas colunas de ligação,
--  criado pela chave estrangeira; os ix_see_itens_* e ix_see_eventos_proc do modelo são
--  redundantes e não precisam ser criados.)
