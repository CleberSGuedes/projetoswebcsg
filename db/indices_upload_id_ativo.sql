-- Índices para os relatórios de execução (04-09/11/2026).
--
-- Por quê: as tabelas só tinham a chave primária. `WHERE upload_id = ?` (painel inicial,
-- Limpeza de Histórico, API /meta) e `WHERE ativo = 1` (todos os relatórios; só ~5% das
-- linhas de emp/nob estão ativas) varriam a tabela inteira: emp 1,2 mi e nob 1,5 mi de linhas.
-- O painel chegou a levar ~30 s para abrir.
--
-- Como aplicar: um comando por vez, fora do horário de uso e da carga das 5h. Sem trava de
-- leitura/escrita (ALGORITHM=INPLACE, LOCK=NONE). Ordem: tabelas pequenas, depois emp e nob.
-- plan51 já tem os dois (ix_plan51_upload_id e ix_plan51_exercicio_ativo).
--
-- Desfazer: os DROP INDEX do final.

-- Pequenas
ALTER TABLE est_emp ADD INDEX ix_est_emp_upload_id (upload_id), ALGORITHM=INPLACE, LOCK=NONE;
ALTER TABLE ped     ADD INDEX ix_ped_upload_id (upload_id),     ALGORITHM=INPLACE, LOCK=NONE;
ALTER TABLE fip613  ADD INDEX ix_fip613_upload_id (upload_id),  ALGORITHM=INPLACE, LOCK=NONE;

-- Grandes (3,5 e 3,8 GB): uma de cada vez
ALTER TABLE emp ADD INDEX ix_emp_upload_id (upload_id), ALGORITHM=INPLACE, LOCK=NONE;
ALTER TABLE emp ADD INDEX ix_emp_ativo (ativo),         ALGORITHM=INPLACE, LOCK=NONE;
ALTER TABLE nob ADD INDEX ix_nob_upload_id (upload_id), ALGORITHM=INPLACE, LOCK=NONE;
ALTER TABLE nob ADD INDEX ix_nob_ativo (ativo),         ALGORITHM=INPLACE, LOCK=NONE;

-- Desfazer
-- ALTER TABLE est_emp DROP INDEX ix_est_emp_upload_id;
-- ALTER TABLE ped     DROP INDEX ix_ped_upload_id;
-- ALTER TABLE fip613  DROP INDEX ix_fip613_upload_id;
-- ALTER TABLE emp DROP INDEX ix_emp_upload_id;
-- ALTER TABLE emp DROP INDEX ix_emp_ativo;
-- ALTER TABLE nob DROP INDEX ix_nob_upload_id;
-- ALTER TABLE nob DROP INDEX ix_nob_ativo;
