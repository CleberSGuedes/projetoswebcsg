-- Índices recomendados para reduzir custo da /api/meta-fisica/options
-- Aplicar em janela de manutenção.

-- plan21_nger: consulta por filtros da meta física e região
CREATE INDEX idx_plan21_meta_f_filtros
ON plan21_nger (
  ativo,
  exercicio,
  unidade_orcamentaria(50),  -- Ajustado com limite de prefixo
  programa,
  acao_paoe,
  produto_acao(50),         -- Ajustado com limite de prefixo
  unid_medida_produto(10),   -- Ajustado com limite de prefixo
  regiao_produto(50)         -- Ajustado com limite de prefixo
);

-- alterar_meta: reconstrução de histórico aprovado
CREATE INDEX idx_alter_meta_hist_filtros
ON alterar_meta (
  ativo,
  status_aprovacao,
  exercicio,
  unidade_orcamentaria(50),  -- Ajustado com limite de prefixo
  programa,
  acao_paoe,
  produto_acao(50),         -- Ajustado com limite de prefixo
  unid_medida_produto(10),   -- Ajustado com limite de prefixo
  id
);

-- alterar_meta_item: consultas por registro/região/tipo
CREATE INDEX idx_alterar_meta_item_lookup
ON alterar_meta_item (
  alterar_meta_id,
  ativo,
  regiao_codigo(20),        -- Ajustado com limite de prefixo
  tipo
);