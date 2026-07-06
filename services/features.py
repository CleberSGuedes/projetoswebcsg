FEATURES = [
    {
        "id": "dashboard",
        "nome": "Início",
        "locked": True,
        "children": [],
    },
    {
        "id": "logout",
        "nome": "Sair",
        "locked": True,
        "children": [],
    },
    {
        "id": "usuarios",
        "nome": "Usuários",
        "children": [
            {"id": "usuarios/cadastrar", "nome": "Cadastrar"},
            {"id": "usuarios/editar", "nome": "Editar"},
            {"id": "usuarios/perfil", "nome": "Perfil"},
            {"id": "usuarios/senha", "nome": "Alterar Senha"},
            {"id": "usuarios/api-acessos", "nome": "API de Acessos"},
        ],
    },
    {
        "id": "painel",
        "nome": "Painel - Permissões",
        "children": [],
    },
    {
        "id": "atualizar",
        "nome": "Atualizar",
        "children": [
            {"id": "atualizar/fip613", "nome": "FIP 613"},
            {"id": "atualizar/ped", "nome": "PED"},
            {"id": "atualizar/emp", "nome": "EMP"},
            {"id": "atualizar/est-emp", "nome": "EST EMP"},
            {"id": "atualizar/nob", "nome": "NOB"},
            {"id": "atualizar/plan20-seduc", "nome": "PLAN20 - SEDUC"},
            {"id": "atualizar/teto-seduc", "nome": "Teto - SEDUC"},
            {
                "id": "atualizar/estrutura-planejamento/programas",
                "nome": "Programas",
            },
            {
                "id": "atualizar/estrutura-planejamento/acoes",
                "nome": "Ações/PAOE",
            },
            {
                "id": "atualizar/estrutura-planejamento/produtos",
                "nome": "Produtos da Ação",
            },
            {
                "id": "atualizar/estrutura-planejamento/componentes-rev",
                "nome": "Componentes da Revista",
            },
            {
                "id": "atualizar/estrutura-planejamento/componentes",
                "nome": "Componentes",
            },
            {
                "id": "atualizar/estrutura-planejamento/vinculos",
                "nome": "Gerenciar Vínculos",
            },
            {
                "id": "atualizar/estrutura-planejamento/modelos-chave",
                "nome": "Modelos de Chave",
            },
            {
                "id": "atualizar/estrutura-planejamento/catalogo-chave",
                "nome": "Catálogo de Chaves",
            },
            {
                "id": "atualizar/estrutura-planejamento/replicar-exercicio",
                "nome": "Replicar exercício",
            },
            {"id": "atualizar/chave_planejamento_regra", "nome": "Regras Chave Planejamento"},
            {
                "id": "atualizar/chaves_planejamento_upload",
                "nome": "Upload anual de chaves de planejamento",
            },
        ],
    },
    {
        "id": "cadastrar",
        "nome": "Cadastrar",
        "children": [
            {"id": "cadastrar/dotacao", "nome": "Dotação"},
            {"id": "cadastrar/est-dotacao", "nome": "Estorno de Dotação"},
            {"id": "cadastrar/plan_21-nger/meta_fisica", "nome": "Meta Física"},
            {"id": "cadastrar/plan_21-nger/subacao", "nome": "Subação"},
            {"id": "cadastrar/plan_21-nger/etapa", "nome": "Etapa"},
        ],
    },
    {
        "id": "institucional",
        "nome": "Institucional",
        "children": [
            {"id": "institucional/diretrizes", "nome": "Diretrizes e Procedimentos"},
            {"id": "institucional/repositorio", "nome": "Repositório de Arquivos"},
            {"id": "institucional/legislacao", "nome": "Legislação e Normas"},
            {"id": "institucional/parceiros", "nome": "Rede de Parceiros"},
        ],
    },
    {
        "id": "relatorios",
        "nome": "Relatórios",
        "children": [
            {"id": "relatorios/fip613", "nome": "FIP 613"},
            {"id": "relatorios/plan20-seduc", "nome": "PLAN20 - SEDUC"},
            {"id": "relatorios/plan21-nger", "nome": "PLAN21_NGER"},
            {
                "id": "relatorios/estrutura-planejamento",
                "nome": "Consultar Estrutura",
            },
            {"id": "relatorios/ped", "nome": "PED"},
            {"id": "relatorios/emp", "nome": "EMP"},
            {"id": "relatorios/est-emp", "nome": "EST EMP"},
            {"id": "relatorios/nob", "nome": "NOB"},
            {"id": "relatorios/dotacao", "nome": "DOTAÇÃO"},
            {"id": "relatorios/est-dotacao", "nome": "ESTORNO DE DOTAÇÃO"},
        ],
    },
    {
        "id": "paineis-dashboards",
        "nome": "Painéis/Dashboards",
        "children": [
            {"id": "paineis-dashboards/teto-orcamentario", "nome": "Teto Orçamentário"},
        ],
    },
    {
        "id": "area-uens",
        "nome": "Área UENs",
        "children": [
            {
                "id": "area-uens/sage",
                "nome": "SAGE",
                "children": [
                    {"id": "area-uens/sage/notas-see", "nome": "Notas SEE"},
                ],
            },
        ],
    },
]


def flatten_features(features=None):
    """Return a flat list of feature ids (including children)."""
    if features is None:
        features = FEATURES
    flat = []
    for f in features:
        flat.append(f["id"])
        if f.get("children"):
            flat.extend(flatten_features(f["children"]))
    return flat


def build_parent_map(features=None):
    """Return dict child_id -> parent_id using FEATURES tree."""
    if features is None:
        features = FEATURES
    parent_map = {}
    for f in features:
        if f.get("children"):
            for child in f["children"]:
                parent_map[child["id"]] = f["id"]
            parent_map.update(build_parent_map(f["children"]))
    return parent_map
