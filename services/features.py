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
            {"id": "usuarios/senha", "nome": "Alterar senha"},
            {"id": "usuarios/api-acessos", "nome": "API de Acessos"},
        ],
    },
    {
        "id": "painel",
        "nome": "Painel",
        "children": [],
    },
    {
        "id": "atualizar",
        "nome": "Atualizar",
        "children": [
            {"id": "atualizar/fip613", "nome": "FIP 613 (Atualizar)"},
            {"id": "atualizar/ped", "nome": "PED"},
            {"id": "atualizar/emp", "nome": "EMP"},
            {"id": "atualizar/est-emp", "nome": "Est Emp"},
            {"id": "atualizar/nob", "nome": "NOB"},
            {"id": "atualizar/plan20-seduc", "nome": "Plan20 - SEDUC"},
            {"id": "atualizar/chave_planejamento_regra", "nome": "Regras Chave Planejamento"},
            {
                "id": "atualizar/chaves_planejamento_upload",
                "nome": "Upload anual chave_planejamento",
            },
        ],
    },
    {
        "id": "cadastrar",
        "nome": "Cadastrar",
        "children": [
            {"id": "cadastrar/dotacao", "nome": "Dotacao"},
            {"id": "cadastrar/est-dotacao", "nome": "Estorno de Dotacao"},
            {"id": "cadastrar/plan_21-nger/meta_fisica", "nome": "Plan 21 - NGER - Meta Fisica"},
            {"id": "cadastrar/plan_21-nger/subacao", "nome": "Plan 21 - NGER - Subacao"},
            {"id": "cadastrar/plan_21-nger/etapa", "nome": "Plan 21 - NGER - Etapa"},
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
            {"id": "relatorios/fip613", "nome": "FIP 613 (Relatório)"},
            {"id": "relatorios/plan20-seduc", "nome": "Plan20 - SEDUC"},
            {"id": "relatorios/plan21-nger", "nome": "Plan21_NGER"},
            {"id": "relatorios/ped", "nome": "PED"},
            {"id": "relatorios/emp", "nome": "EMP"},
            {"id": "relatorios/est-emp", "nome": "Est Emp"},
            {"id": "relatorios/nob", "nome": "NOB"},
            {"id": "relatorios/dotacao", "nome": "Dotação"},
            {"id": "relatorios/est-dotacao", "nome": "Estorno de Dotação"},
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
            flat.extend([c["id"] for c in f["children"]])
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
    return parent_map
