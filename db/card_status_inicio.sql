-- Card "Status" da tela Início passou a ser controlado pelo Painel de Permissões
-- (funcionalidade dashboard/card-status). Antes era fixo para os niveis 1 e 2.
-- Nivel 1 (admin) ve sempre. Este script mantem o comportamento anterior para o nivel 2;
-- depois ajuste pelo Painel - Permissoes (perfil/nivel/usuario).
INSERT INTO nivel_permissoes (nivel, feature, ativo)
SELECT 2, 'dashboard/card-status', 1
WHERE NOT EXISTS (
  SELECT 1 FROM nivel_permissoes WHERE nivel = 2 AND feature = 'dashboard/card-status'
);
