-- Notas SEE: exercício do catálogo (MySQL/MariaDB).
-- Antes o exercício era escrito no nome do catálogo; passa a ser uma coluna própria,
-- e o nome deixa de ser único no sistema para ser único dentro do exercício.

ALTER TABLE see_catalogos ADD COLUMN exercicio SMALLINT NULL AFTER nome;

-- Preenche os catálogos existentes com o ano escrito no nome; sem ano, usa o ano de criação.
UPDATE see_catalogos
   SET exercicio = CAST(REGEXP_SUBSTR(nome, '20[0-9]{2}') AS UNSIGNED)
 WHERE exercicio IS NULL AND nome REGEXP '20[0-9]{2}';
UPDATE see_catalogos SET exercicio = YEAR(created_at) WHERE exercicio IS NULL;

ALTER TABLE see_catalogos MODIFY exercicio SMALLINT NOT NULL;
ALTER TABLE see_catalogos
  DROP INDEX uq_see_catalogo_nome,
  ADD UNIQUE KEY uq_see_catalogo_exercicio_nome (exercicio, nome);
